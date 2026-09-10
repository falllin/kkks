package io.nexusstore.common;

import com.fasterxml.jackson.databind.JsonNode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.TooLongFrameException;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.net.ServerSocket;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

class RpcTransportTest {
    @Test void roundTripConcurrentRequestsReuseConnectionAndCarryOneMiBFile() throws Exception {
        try (RpcServer server = new RpcServer("127.0.0.1", 0,
                request -> CompletableFuture.completedFuture(RpcResponse.ok(request.id(), request.body())));
             RpcClient client = new RpcClient(Duration.ofSeconds(5))) {
            server.start();
            String address = address(server);
            List<CompletableFuture<JsonNode>> calls = new ArrayList<>();
            for (int i = 0; i < 100; i++) calls.add(client.call(address, "echo", "s0", Map.of("index", i)));
            for (int i = 0; i < calls.size(); i++) assertEquals(i, calls.get(i).get(6, TimeUnit.SECONDS).path("index").asInt());
            String payload = Base64.getEncoder().encodeToString(new byte[1024 * 1024]);
            JsonNode echoed = client.call(address, "put", "s1", Map.of("data", payload)).get(6, TimeUnit.SECONDS);
            assertEquals(payload, echoed.path("data").asText());
            assertEquals(1, client.connectionCount());
            await(() -> client.pendingCount() == 0);
        }
    }

    @Test void businessErrorsPreserveLeaderAndHandlerExceptionsBecomeRpcErrors() throws Exception {
        try (RpcServer server = new RpcServer("127.0.0.1", 0, request -> {
            if (request.method().equals("throw")) throw new IllegalStateException("disk unavailable");
            return CompletableFuture.completedFuture(RpcResponse.fail(request.id(), "not leader", "n2"));
        }); RpcClient client = new RpcClient(Duration.ofSeconds(2))) {
            server.start();
            RpcException redirect = rpcError(client.call(address(server), "put", "s0", Map.of()));
            assertEquals("not leader", redirect.getMessage());
            assertEquals("n2", redirect.leaderId());
            assertEquals("disk unavailable", rpcError(client.call(address(server), "throw", "s0", Map.of())).getMessage());
            await(() -> client.pendingCount() == 0);
        }
    }

    @Test void timedOutAndCancelledRequestsAreRemovedAndLateResponseIsIgnored() throws Exception {
        CompletableFuture<RpcResponse> delayed = new CompletableFuture<>();
        java.util.concurrent.atomic.AtomicLong delayedId = new java.util.concurrent.atomic.AtomicLong();
        try (RpcServer server = new RpcServer("127.0.0.1", 0, request -> {
            if (request.method().equals("hang")) {
                delayedId.set(request.id());
                return delayed;
            }
            return CompletableFuture.completedFuture(RpcResponse.ok(request.id(), Map.of("alive", true)));
        }); RpcClient client = new RpcClient(Duration.ofMillis(200))) {
            server.start();
            assertTrue(rpcError(client.call(address(server), "hang", "s0", Map.of())).getMessage().contains("timeout"));
            await(() -> client.pendingCount() == 0);
            delayed.complete(RpcResponse.ok(delayedId.get(), Map.of("late", true)));
            assertTrue(client.call(address(server), "echo", "s0", Map.of()).get(2, TimeUnit.SECONDS).path("alive").asBoolean());
            CompletableFuture<JsonNode> cancelled = client.call(address(server), "echo", "s0", Map.of());
            cancelled.cancel(false);
            await(() -> client.pendingCount() == 0);
        }
    }

    @Test void disconnectFailsPendingAndLaterCallReconnectsToRestartedServer() throws Exception {
        AtomicInteger received = new AtomicInteger();
        RpcServer first = new RpcServer("127.0.0.1", 0, request -> {
            received.incrementAndGet();
            return new CompletableFuture<>();
        });
        try (RpcClient client = new RpcClient(Duration.ofSeconds(10))) {
            first.start();
            int port = first.port();
            CompletableFuture<JsonNode> interrupted = client.call(address(first), "hang", "s0", Map.of());
            await(() -> received.get() == 1);
            first.close();
            assertTrue(rpcError(interrupted).getMessage().contains("closed"));
            await(() -> client.pendingCount() == 0 && client.connectionCount() == 0);
            try (RpcServer restarted = new RpcServer("127.0.0.1", port, request ->
                    CompletableFuture.completedFuture(RpcResponse.ok(request.id(), Map.of("restarted", true))))) {
                restarted.start();
                assertTrue(client.call(address(restarted), "echo", "s0", Map.of()).get(3, TimeUnit.SECONDS)
                        .path("restarted").asBoolean());
            }
        } finally {
            first.close();
        }
    }

    @Test void decoderHandlesSplitHeadersBodiesAndCoalescedFrames() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel();
        RpcFrames.install(channel.pipeline());
        byte[] first = Json.MAPPER.writeValueAsBytes(new RpcRequest(1, "get", "s0", Json.MAPPER.valueToTree(Map.of("key", "a"))));
        byte[] second = Json.MAPPER.writeValueAsBytes(new RpcRequest(2, "get", "s0", Json.MAPPER.valueToTree(Map.of("key", "b"))));
        ByteBuf both = Unpooled.buffer().writeInt(first.length).writeBytes(first).writeInt(second.length).writeBytes(second);
        try {
            assertFalse(channel.writeInbound(both.readRetainedSlice(2)));
            assertFalse(channel.writeInbound(both.readRetainedSlice(5)));
            assertTrue(channel.writeInbound(both.readRetainedSlice(both.readableBytes())));
            ByteBuf frame1 = channel.readInbound();
            ByteBuf frame2 = channel.readInbound();
            try {
                assertEquals(1, RpcFrames.decode(frame1, RpcRequest.class).id());
                assertEquals(2, RpcFrames.decode(frame2, RpcRequest.class).id());
            } finally {
                frame1.release();
                frame2.release();
            }
            assertNull(channel.readInbound());
        } finally {
            both.release();
            channel.finishAndReleaseAll();
        }
    }

    @Test void rawSocketFramesRoundTripWithNetworkByteOrder() throws Exception {
        try (RpcServer server = new RpcServer("127.0.0.1", 0, request ->
                CompletableFuture.completedFuture(RpcResponse.ok(request.id(), request.body())))) {
            server.start();
            try (Socket socket = new Socket("127.0.0.1", server.port())) {
                socket.setSoTimeout(3000);
                DataOutputStream output = new DataOutputStream(socket.getOutputStream());
                DataInputStream input = new DataInputStream(socket.getInputStream());
                for (int id = 1; id <= 2; id++) {
                    byte[] json = Json.MAPPER.writeValueAsBytes(new RpcRequest(id, "echo", "s0", Json.MAPPER.valueToTree(Map.of("name", "中文"))));
                    output.writeInt(json.length);
                    output.write(json, 0, 3);
                    output.flush();
                    output.write(json, 3, json.length - 3);
                }
                output.flush();
                for (int id = 1; id <= 2; id++) {
                    RpcResponse response = Json.MAPPER.readValue(input.readNBytes(input.readInt()), RpcResponse.class);
                    assertEquals(id, response.id());
                    assertEquals("中文", response.body().path("name").asText());
                }
            }
        }
    }

    @Test void rejectsFramesOverLimitWithoutAllocatingBody() {
        EmbeddedChannel channel = new EmbeddedChannel();
        RpcFrames.install(channel.pipeline());
        try {
            assertThrows(TooLongFrameException.class,
                    () -> channel.writeInbound(Unpooled.buffer(4).writeInt(RpcFrames.MAX_BYTES + 1)));
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test void closingClientFailsRequestsAndRejectsNewCalls() throws Exception {
        try (RpcServer server = new RpcServer("127.0.0.1", 0, request -> new CompletableFuture<>());
             RpcClient client = new RpcClient(Duration.ofSeconds(10))) {
            server.start();
            CompletableFuture<JsonNode> waiting = client.call(address(server), "hang", "s0", Map.of());
            client.close();
            assertTrue(rpcError(waiting).getMessage().contains("closed"));
            assertTrue(rpcError(client.call(address(server), "get", "s0", Map.of())).getMessage().contains("closed"));
            assertEquals(0, client.pendingCount());
        }
    }

    @Test void pendingLimitRejectsExcessWorkAndCancellationReturnsCapacity() throws Exception {
        // A real peer that accepts TCP but never replies isolates the client's admission limit.
        try (ServerSocket listener = new ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"));
             RpcClient client = new RpcClient(Duration.ofSeconds(10))) {
            listener.setSoTimeout(3000);
            String address = "127.0.0.1:" + listener.getLocalPort();
            List<CompletableFuture<JsonNode>> calls = new ArrayList<>();
            calls.add(client.call(address, "hang", "s0", Map.of()));
            try (Socket peer = listener.accept()) {
                for (int i = 1; i < 1024; i++) calls.add(client.call(address, "hang", "s0", Map.of()));
                assertEquals(1024, client.pendingCount());
                assertTrue(rpcError(client.call(address, "hang", "s0", Map.of())).getMessage().contains("overloaded"));
                calls.forEach(call -> call.cancel(false));
                await(() -> client.pendingCount() == 0);
                CompletableFuture<JsonNode> next = client.call(address, "hang", "s0", Map.of());
                assertFalse(next.isCompletedExceptionally());
                next.cancel(false);
            }
        }
    }

    private static RpcException rpcError(CompletableFuture<?> future) {
        ExecutionException failure = assertThrows(ExecutionException.class, () -> future.get(3, TimeUnit.SECONDS));
        return assertInstanceOf(RpcException.class, failure.getCause());
    }

    private static String address(RpcServer server) { return "127.0.0.1:" + server.port(); }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(10);
        assertTrue(condition.getAsBoolean(), "condition did not become true within 3 seconds");
    }
}
