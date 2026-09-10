package io.nexusstore.common;

import com.fasterxml.jackson.databind.JsonNode;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Multiplexed, asynchronous RPC. No method waits on a Netty event loop. */
public final class RpcClient implements AutoCloseable {
    private static final int MAX_PENDING = 1024;
    private static final int MAX_ADDRESSES = 128;
    private final long timeoutMillis;
    private final NioEventLoopGroup eventLoops;
    private final ConcurrentHashMap<String, CompletableFuture<Channel>> connections = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Pending> pending = new ConcurrentHashMap<>();
    private final Semaphore capacity = new Semaphore(MAX_PENDING);
    private final AtomicLong sequence = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();

    public RpcClient(Duration timeout) {
        timeoutMillis = Objects.requireNonNull(timeout, "timeout").toMillis();
        if (timeoutMillis < 1 || timeoutMillis > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("timeout must be between 1 ms and Integer.MAX_VALUE ms");
        }
        eventLoops = new NioEventLoopGroup(2);
    }

    public CompletableFuture<JsonNode> call(String address, String method, String group, Object body) {
        if (closed.get()) return failed("RPC client closed");
        if (!capacity.tryAcquire()) return failed("RPC client overloaded: 1024 requests in flight");
        long id = sequence.incrementAndGet();
        Pending request = new Pending();
        pending.put(id, request);
        request.result.whenComplete((ignored, error) -> {
            if (pending.remove(id, request)) capacity.release();
            ScheduledFuture<?> timer = request.timer;
            if (timer != null) timer.cancel(false);
        });
        try {
            Objects.requireNonNull(method, "method");
            Objects.requireNonNull(group, "group");
            RpcRequest message = new RpcRequest(id, method, group, Json.MAPPER.valueToTree(body));
            request.timer = eventLoops.next().schedule(() -> request.result.completeExceptionally(
                    new RpcException("RPC timeout after " + timeoutMillis + " ms: " + method, null)),
                    timeoutMillis, TimeUnit.MILLISECONDS);
            if (request.result.isDone()) request.timer.cancel(false);
            connectionFor(address).whenComplete((channel, connectError) -> {
                if (request.result.isDone()) return;
                if (connectError != null) {
                    request.result.completeExceptionally(new RpcException("RPC connect failed: " + connectError.getMessage(), null));
                    return;
                }
                request.channel = channel;
                if (!channel.isActive()) {
                    request.result.completeExceptionally(new RpcException("RPC connection closed", null));
                    return;
                }
                // Execute the check and write together, so concurrent callers cannot race the watermark.
                channel.eventLoop().execute(() -> {
                    if (request.result.isDone()) return;
                    if (!channel.isActive() || !channel.isWritable()) {
                        request.result.completeExceptionally(new RpcException("RPC connection unavailable or write buffer full", null));
                        return;
                    }
                    try {
                        channel.writeAndFlush(RpcFrames.encode(message)).addListener(written -> {
                            if (!written.isSuccess()) request.result.completeExceptionally(
                                    new RpcException("RPC write failed: " + written.cause().getMessage(), null));
                        });
                    } catch (Exception error) {
                        request.result.completeExceptionally(new RpcException("RPC encode failed: " + error.getMessage(), null));
                    }
                });
            });
        } catch (Exception error) {
            request.result.completeExceptionally(new RpcException("RPC request failed: " + error.getMessage(), null));
        }
        return request.result;
    }

    private synchronized CompletableFuture<Channel> connectionFor(String address) {
        if (closed.get()) return CompletableFuture.failedFuture(new RpcException("RPC client closed", null));
        URI endpoint = URI.create("tcp://" + Objects.requireNonNull(address, "address"));
        if (endpoint.getHost() == null || endpoint.getPort() < 1 || endpoint.getPort() > 65535
                || endpoint.getUserInfo() != null || endpoint.getQuery() != null || endpoint.getFragment() != null
                || !endpoint.getPath().isEmpty()) {
            return CompletableFuture.failedFuture(new RpcException("Expected host:port address", null));
        }
        CompletableFuture<Channel> existing = connections.get(address);
        if (existing != null) {
            if (!existing.isDone()) return existing;
            if (!existing.isCompletedExceptionally() && existing.getNow(null).isActive()) return existing;
            connections.remove(address, existing);
        }
        if (connections.size() >= MAX_ADDRESSES) {
            return CompletableFuture.failedFuture(new RpcException("RPC connection cache full: 128 addresses", null));
        }
        CompletableFuture<Channel> connected = new CompletableFuture<>();
        connections.put(address, connected);
        Bootstrap bootstrap = new Bootstrap().group(eventLoops).channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) timeoutMillis)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(2 * 1024 * 1024, 4 * 1024 * 1024))
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override protected void initChannel(SocketChannel channel) {
                        RpcFrames.install(channel.pipeline());
                        channel.pipeline().addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override protected void channelRead0(ChannelHandlerContext context, ByteBuf frame) throws Exception {
                                RpcResponse response = RpcFrames.decode(frame, RpcResponse.class);
                                Pending waiting = pending.get(response.id());
                                if (waiting == null || waiting.channel != context.channel()) return;
                                if (response.success()) waiting.result.complete(response.body());
                                else waiting.result.completeExceptionally(new RpcException(response.error(), response.leaderId()));
                            }

                            @Override public void channelInactive(ChannelHandlerContext context) {
                                connections.remove(address, connected);
                                failChannel(context.channel(), "RPC connection closed");
                            }

                            @Override public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
                                failChannel(context.channel(), "RPC protocol error: " + cause.getMessage());
                                context.close();
                            }
                        });
                    }
                });
        bootstrap.connect(endpoint.getHost(), endpoint.getPort()).addListener(future -> {
            if (future.isSuccess()) {
                Channel channel = ((io.netty.channel.ChannelFuture) future).channel();
                if (closed.get()) {
                    channel.close();
                    connected.completeExceptionally(new RpcException("RPC client closed", null));
                } else connected.complete(channel);
            } else {
                connections.remove(address, connected);
                connected.completeExceptionally(future.cause());
            }
        });
        return connected;
    }

    private void failChannel(Channel channel, String message) {
        pending.values().forEach(request -> {
            if (request.channel == channel) request.result.completeExceptionally(new RpcException(message, null));
        });
    }

    private static CompletableFuture<JsonNode> failed(String message) {
        return CompletableFuture.failedFuture(new RpcException(message, null));
    }

    @Override public synchronized void close() {
        if (!closed.compareAndSet(false, true)) return;
        pending.values().forEach(request -> request.result.completeExceptionally(new RpcException("RPC client closed", null)));
        connections.values().forEach(connection -> connection.thenAccept(Channel::close));
        connections.clear();
        eventLoops.shutdownGracefully(0, 2, TimeUnit.SECONDS);
    }

    // Package-private metrics let tests prove cleanup and connection reuse without exposing mutable internals.
    int pendingCount() { return pending.size(); }
    int connectionCount() { return connections.size(); }

    private static final class Pending {
        final CompletableFuture<JsonNode> result = new CompletableFuture<>();
        volatile Channel channel;
        volatile ScheduledFuture<?> timer;
    }
}
