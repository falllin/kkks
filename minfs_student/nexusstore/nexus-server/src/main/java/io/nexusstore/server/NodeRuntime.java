package io.nexusstore.server;

import com.fasterxml.jackson.databind.JsonNode;
import io.nexusstore.common.*;
import io.nexusstore.raft.*;
import io.nexusstore.raft.RaftMessages.*;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/** Wires independent Raft groups to one Netty listener. No HTTP-to-HTTP forwarding. */
@Component
public class NodeRuntime implements AutoCloseable {
    private final NexusProperties config;
    private final PeerDirectory directory;
    private final ConsistentHashRing ring;
    private final Map<String,RaftNode> groups = new LinkedHashMap<>();
    private final Map<String,String> leaders = new ConcurrentHashMap<>();
    private final RpcClient rpc = new RpcClient(Duration.ofMillis(3200));
    private final ScheduledThreadPoolExecutor retries = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "file-routing"));
    private final Semaphore admission = new Semaphore(64);
    private final Set<CompletableFuture<CommandResult>> activeRequests = ConcurrentHashMap.newKeySet();
    private RpcServer server;
    private FileChannel lockChannel;
    private FileLock lock;
    private volatile boolean running;

    public NodeRuntime(NexusProperties config, PeerDirectory directory) {
        this.config = config;
        this.directory = directory;
        this.ring = new ConsistentHashRing(config.getShardCount(), config.getVirtualNodes());
        retries.setRemoveOnCancelPolicy(true);
        retries.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    @PostConstruct
    public void start() throws Exception {
        try {
            Path root = Path.of(config.getDataDir()).toAbsolutePath().normalize();
            Files.createDirectories(root);
            lockChannel = FileChannel.open(root.resolve("node.lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            lock = lockChannel.tryLock();
            if (lock == null) throw new IllegalStateException("Data directory is already in use: " + root);
            verifyTopology(root);
            RaftTransport transport = new RaftTransport() {
                public CompletableFuture<VoteResponse> requestVote(String peer, String group, VoteRequest request) {
                    return rpc.call(directory.address(peer), "VOTE", group, request).thenApply(v -> Json.MAPPER.convertValue(v, VoteResponse.class));
                }
                public CompletableFuture<AppendResponse> appendEntries(String peer, String group, AppendRequest request) {
                    return rpc.call(directory.address(peer), "APPEND", group, request).thenApply(v -> Json.MAPPER.convertValue(v, AppendResponse.class));
                }
                public CompletableFuture<InstallSnapshotResponse> installSnapshot(String peer, String group, InstallSnapshotRequest request) {
                    return rpc.call(directory.address(peer), "SNAPSHOT", group, request)
                            .thenApply(v -> Json.MAPPER.convertValue(v, InstallSnapshotResponse.class));
                }
            };
            for (int i = 0; i < config.getShardCount(); i++) {
                String id = "s" + i;
                groups.put(id, new RaftNode(config.getNodeId(), id, List.copyOf(directory.seeds().keySet()), root.resolve(id), transport));
            }
            server = new RpcServer(config.getHost(), config.getRpcPort(), this::handle);
            server.start();
            groups.values().forEach(RaftNode::start);
            running = true;
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    private void verifyTopology(Path root) throws IOException {
        JsonNode expected = Json.MAPPER.valueToTree(Map.of("nodeId", config.getNodeId(), "peers", directory.seeds(),
                "shardCount", config.getShardCount(), "virtualNodes", config.getVirtualNodes(), "format", 1));
        Path path = root.resolve("topology.json");
        if (Files.exists(path)) {
            if (!expected.equals(Json.MAPPER.readTree(path.toFile())))
                throw new IllegalStateException("Topology differs from persisted topology.json; changing the ring requires data migration. Use a NEW data directory for a new cluster.");
        } else {
            try (FileChannel file = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                ByteBuffer bytes = ByteBuffer.wrap(Json.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(expected));
                while (bytes.hasRemaining()) file.write(bytes);
                file.force(true);
            }
        }
    }

    private CompletableFuture<RpcResponse> handle(RpcRequest request) {
        try {
            RaftNode node = groups.get(request.group());
            if (node == null) throw new IllegalArgumentException("Unknown shard: " + request.group());
            CompletableFuture<?> work = switch (request.method()) {
                case "VOTE" -> node.onRequestVote(Json.MAPPER.convertValue(request.body(), VoteRequest.class));
                case "APPEND" -> node.onAppendEntries(Json.MAPPER.convertValue(request.body(), AppendRequest.class));
                case "SNAPSHOT" -> node.onInstallSnapshot(Json.MAPPER.convertValue(request.body(), InstallSnapshotRequest.class));
                case "FILE" -> {
                    Command command = Json.MAPPER.convertValue(request.body(), Command.class);
                    validate(command);
                    if (!request.group().equals(shardFor(command.key()))) throw new IllegalArgumentException("Wrong shard for key");
                    yield node.submit(command);
                }
                default -> throw new IllegalArgumentException("Unknown RPC method");
            };
            return work.handle((value, error) -> error == null ? RpcResponse.ok(request.id(), value) : rpcFailure(request.id(), error));
        } catch (Exception e) { return CompletableFuture.completedFuture(rpcFailure(request.id(), e)); }
    }

    private RpcResponse rpcFailure(long id, Throwable error) {
        Throwable cause = unwrap(error);
        if (cause instanceof NotLeaderException n) return RpcResponse.fail(id, "NOT_LEADER: " + n.getMessage(), n.leaderId());
        if (cause instanceof IllegalArgumentException) return RpcResponse.fail(id, "INVALID_ARGUMENT: " + cause.getMessage(), null);
        return RpcResponse.fail(id, "UNAVAILABLE: " + cause.getMessage(), null);
    }

    public String shardFor(String key) { FileRules.validateKey(key); return ring.shardFor(key); }

    /** Every retry keeps the same command ID. An RPC timeout has an unknown commit outcome. */
    public CompletableFuture<CommandResult> execute(Command command) {
        validate(command);
        String group = shardFor(command.key());
        if (!running || !admission.tryAcquire()) return CompletableFuture.failedFuture(new IllegalStateException("Node busy or stopped"));
        CompletableFuture<CommandResult> result = new CompletableFuture<>();
        activeRequests.add(result);
        result.whenComplete((v, e) -> { activeRequests.remove(result); admission.release(); });
        // close() may have finished its active-request scan while this request was being registered.
        if (!running) {
            result.completeExceptionally(new IllegalStateException("Node stopped; write outcome may be unknown"));
            return result;
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(9);
        schedule(result, () -> result.completeExceptionally(new IllegalStateException(
                "No committed response within 9 seconds; write outcome unknown. Retry the same X-Request-Id.")), 9000);
        schedule(result, () -> attempt(group, command, new HashSet<>(), deadline, result), 0);
        return result;
    }

    /** A rejected retry/deadline must complete the request, including its admission cleanup. */
    private void schedule(CompletableFuture<CommandResult> result, Runnable action, long delayMillis) {
        if (result.isDone()) return;
        try {
            ScheduledFuture<?> scheduled = retries.schedule(() -> {
                if (result.isDone()) return;
                try { action.run(); }
                catch (RuntimeException error) {
                    result.completeExceptionally(error instanceof IllegalArgumentException ? error :
                            new IllegalStateException("Request routing failed; write outcome may be unknown", error));
                }
            }, delayMillis, TimeUnit.MILLISECONDS);
            result.whenComplete((value, error) -> scheduled.cancel(false));
        } catch (RejectedExecutionException stopped) {
            result.completeExceptionally(new IllegalStateException("Node stopped; write outcome may be unknown", stopped));
        }
    }

    private void attempt(String group, Command command, Set<String> tried, long deadline, CompletableFuture<CommandResult> result) {
        if (result.isDone()) return;
        if (!running || System.nanoTime() > deadline) {
            result.completeExceptionally(new IllegalStateException("No committed response before deadline; quorum unavailable or election in progress. Retry writes with the SAME X-Request-Id."));
            return;
        }
        String hinted = leaders.get(group);
        if (hinted == null) hinted = groups.get(group).status().leaderId();
        String target = hinted != null && directory.seeds().containsKey(hinted) && !tried.contains(hinted) ? hinted :
                directory.seeds().keySet().stream().filter(n -> !tried.contains(n)).findFirst().orElse(null);
        if (target == null) {
            schedule(result, () -> attempt(group, command, new HashSet<>(), deadline, result), 150);
            return;
        }
        tried.add(target);
        String selected = target;
        CompletableFuture<CommandResult> call = target.equals(config.getNodeId()) ? groups.get(group).submit(command) :
                rpc.call(directory.address(target), "FILE", group, command).thenApply(v -> Json.MAPPER.convertValue(v, CommandResult.class));
        call.whenComplete((value, error) -> {
            if (result.isDone()) return;
            if (error == null) { leaders.put(group, selected); result.complete(value); return; }
            Throwable cause = unwrap(error);
            String leader = cause instanceof NotLeaderException n ? n.leaderId() : cause instanceof RpcException r ? r.leaderId() : null;
            if (leader != null && directory.seeds().containsKey(leader)) leaders.put(group, leader);
            else leaders.remove(group, selected);
            if (cause instanceof IllegalArgumentException || cause instanceof RpcException && String.valueOf(cause.getMessage()).startsWith("INVALID_ARGUMENT:")) {
                result.completeExceptionally(new IllegalArgumentException(cause.getMessage()));
            } else {
                // Avoid deep callback recursion when a local follower rejects immediately.
                schedule(result, () -> attempt(group, command, tried, deadline, result), 0);
            }
        });
    }

    private static void validate(Command command) {
        if (command == null || command.type() == null || command.type() == Command.Type.NOOP) throw new IllegalArgumentException("Expected PUT, GET or DELETE");
        FileRules.validateKey(command.key());
        FileRules.validateRequestId(command.requestId());
        if (command.type() == Command.Type.PUT && (command.value() == null || command.value().length > FileRules.MAX_FILE_BYTES))
            throw new IllegalArgumentException("File must be at most 1 MiB");
        if (command.type() != Command.Type.PUT && command.value() != null) throw new IllegalArgumentException("Only PUT accepts bytes");
    }

    public List<NodeStatus> statuses() { return groups.values().stream().map(RaftNode::status).toList(); }
    /** Local maintenance only: each group independently snapshots its already-applied prefix. */
    public CompletableFuture<List<NodeStatus>> compactAll() {
        if (!running) return CompletableFuture.failedFuture(new IllegalStateException("Node stopped"));
        List<CompletableFuture<NodeStatus>> work = groups.values().stream().map(RaftNode::compact).toList();
        return CompletableFuture.allOf(work.toArray(CompletableFuture[]::new))
                .thenApply(ignored -> work.stream().map(CompletableFuture::join).toList());
    }
    public boolean isRunning() { return running; }
    public String nodeId() { return config.getNodeId(); }
    public int shardCount() { return config.getShardCount(); }
    public static Throwable unwrap(Throwable error) {
        while ((error instanceof CompletionException || error instanceof ExecutionException) && error.getCause() != null) error = error.getCause();
        return error;
    }

    @PreDestroy
    public void close() {
        running = false;
        activeRequests.forEach(f -> f.completeExceptionally(new IllegalStateException("Node stopped; write outcome may be unknown")));
        groups.values().forEach(RaftNode::close);
        if (server != null) server.close();
        rpc.close();
        retries.shutdownNow();
        try { if (lock != null && lock.isValid()) lock.release(); if (lockChannel != null) lockChannel.close(); }
        catch (IOException ignored) { }
    }
}
