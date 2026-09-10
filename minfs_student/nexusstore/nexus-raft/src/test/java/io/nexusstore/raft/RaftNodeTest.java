package io.nexusstore.raft;

import io.nexusstore.raft.RaftMessages.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class RaftNodeTest {
    @TempDir Path directory;
    private static final List<String> MEMBERS = List.of("n1", "n2", "n3");
    private static final String GROUP = "shard-0";

    @Test
    void leaderFailoverAndEntireClusterRestartRetainCommittedData() throws Exception {
        try (Cluster cluster = new Cluster(directory)) {
            RaftNode first = cluster.leader();
            CommandResult write = await(first.submit(put("alpha", "durable", "write-1")));
            assertTrue(write.found());
            assertNull(write.value());
            assertEquals("durable", text(await(first.submit(get("alpha"))).value()));
            cluster.stop(first.status().nodeId());
            RaftNode second = cluster.leader();
            assertNotEquals(first.status().nodeId(), second.status().nodeId());
            assertEquals("durable", text(await(second.submit(get("alpha"))).value()));
            await(second.submit(put("beta", "after-failover", "write-2")));
            cluster.restartAll();
            RaftNode restarted = cluster.leader();
            assertEquals("durable", text(await(restarted.submit(get("alpha"))).value()));
            assertEquals("after-failover", text(await(restarted.submit(get("beta"))).value()));
        }
    }

    @Test
    void isolatedFormerLeaderCannotAcknowledgeReadsOrWritesAndItsTailIsRepaired() throws Exception {
        try (Cluster cluster = new Cluster(directory)) {
            RaftNode oldLeader = cluster.leader();
            await(oldLeader.submit(put("key", "before", "initial")));
            String isolated = oldLeader.status().nodeId();
            cluster.isolate(isolated);
            CompletableFuture<CommandResult> lostWrite = oldLeader.submit(put("key", "uncommitted", "lost-write"));
            CompletableFuture<CommandResult> staleRead = oldLeader.submit(get("key"));
            RaftNode newLeader = cluster.leaderExcept(isolated);
            await(newLeader.submit(put("key", "majority", "majority-write")));
            assertFutureFailsWith(lostWrite, TimeoutException.class);
            assertFutureFailsWith(staleRead, TimeoutException.class);
            assertTrue(oldLeader.status().lastLogIndex() > oldLeader.status().commitIndex());

            cluster.heal();
            eventually(() -> oldLeader.status().role().equals("FOLLOWER"), 5000);
            long target = await(newLeader.submit(get("key"))).index();
            eventually(() -> oldLeader.status().lastApplied() >= target, 5000);
            assertEquals("majority", text(await(newLeader.submit(get("key"))).value()));
            assertFutureFailsWith(oldLeader.submit(get("key")), NotLeaderException.class);

            // Force the repaired node to prove recovery via a new election and a majority-backed GET.
            cluster.stop(newLeader.status().nodeId());
            assertEquals("majority", text(await(cluster.leader().submit(get("key"))).value()));
        }
    }

    @Test
    void mutationRequestIdsAreIdempotentAcrossChangesFailoverAndRestart() throws Exception {
        try (Cluster cluster = new Cluster(directory)) {
            RaftNode leader = cluster.leader();
            Command first = put("key", "one", "same-id");
            CommandResult original = await(leader.submit(first));
            await(leader.submit(put("key", "two", "next-id")));
            CommandResult retry = await(leader.submit(first));
            assertEquals(original.index(), retry.index());
            assertEquals("two", text(await(leader.submit(get("key"))).value()));
            assertFutureFailsWith(leader.submit(put("key", "different", "same-id")), IllegalArgumentException.class);
            Command deletion = new Command(Command.Type.DELETE, "key", null, "delete-id");
            assertTrue(await(leader.submit(deletion)).found());
            await(leader.submit(put("key", "three", "third-id")));
            assertTrue(await(leader.submit(deletion)).found());
            assertEquals("three", text(await(leader.submit(get("key"))).value()));
            cluster.restartAll();
            leader = cluster.leader();
            assertEquals(original.index(), await(leader.submit(first)).index());
            assertEquals("three", text(await(leader.submit(get("key"))).value()));
        }
    }

    @Test
    void votesAndTermsSurviveRestartAndOnlyUpToDateCandidatesWinVotes() throws Exception {
        Path path = directory.resolve("one");
        RaftNode node = loneNode(path);
        try {
            assertTrue(await(node.onRequestVote(new VoteRequest(10, "n2", 0, 0))).voteGranted());
        } finally { node.close(); }
        node = loneNode(path);
        try {
            assertFalse(await(node.onRequestVote(new VoteRequest(10, "n3", 0, 0))).voteGranted());
            assertTrue(await(node.onRequestVote(new VoteRequest(10, "n2", 0, 0))).voteGranted());
            LogEntry entry = new LogEntry(1, 10, put("key", "value", "id"));
            assertTrue(await(node.onAppendEntries(new AppendRequest(10, "n2", 0, 0, List.of(entry), 1))).success());
            assertFalse(await(node.onRequestVote(new VoteRequest(11, "n3", 0, 0))).voteGranted());
            assertTrue(await(node.onRequestVote(new VoteRequest(11, "n3", 1, 10))).voteGranted());
            assertFalse(await(node.onRequestVote(new VoteRequest(9, "n2", 1, 9))).voteGranted());
        } finally { node.close(); }
    }

    @Test
    void partialAppendDoesNotCommitUnverifiedOldTailAndCommittedEntriesCannotBeOverwritten() throws Exception {
        try (RaftNode node = loneNode(directory.resolve("one"))) {
            List<LogEntry> oldLog = List.of(
                    new LogEntry(1, 1, put("a", "one", "a-1")),
                    new LogEntry(2, 1, put("b", "old", "b-old")),
                    new LogEntry(3, 1, put("c", "old", "c-old")));
            assertTrue(await(node.onAppendEntries(new AppendRequest(1, "n2", 0, 0, oldLog, 0))).success());
            assertTrue(await(node.onAppendEntries(new AppendRequest(2, "n3", 0, 0,
                    List.of(oldLog.get(0)), 3))).success());
            eventually(() -> node.status().commitIndex() == 1, 500);
            assertEquals(1, node.status().keyCount());
            List<LogEntry> replacement = List.of(new LogEntry(2, 2, put("b", "new", "b-new")));
            assertTrue(await(node.onAppendEntries(new AppendRequest(2, "n3", 1, 1, replacement, 2))).success());
            eventually(() -> node.status().lastApplied() == 2, 500);
            assertEquals(2, node.status().lastLogIndex());
            assertFalse(await(node.onAppendEntries(new AppendRequest(3, "n2", 0, 0,
                    List.of(new LogEntry(1, 3, put("a", "bad", "a-bad"))), 1))).success());
            assertEquals(2, node.status().commitIndex());
        }
    }

    @Test
    void majorityOfAnOldTermEntryDoesNotCommitWithoutCurrentTermEntry() throws Exception {
        Path path = directory.resolve("one");
        try (RaftNode follower = loneNode(path)) {
            List<LogEntry> oldEntries = new ArrayList<>();
            for (int index = 1; index <= 16; index++) {
                oldEntries.add(new LogEntry(index, 1, put("old-" + index, "entry", "old-id-" + index)));
            }
            await(follower.onAppendEntries(new AppendRequest(1, "n2", 0, 0,
                    oldEntries, 0)));
        }
        // n2 acknowledges a full batch of 16 old-term entries, then cannot receive the new NOOP.
        // A buggy "majority replicated means committed" implementation would commit all 16 here.
        CountDownLatch noopBlockedAfterOldBatch = new CountDownLatch(1);
        AtomicBoolean initialProbeRejected = new AtomicBoolean();
        RaftTransport transport = new RaftTransport() {
            @Override public CompletableFuture<VoteResponse> requestVote(String peer, String group, VoteRequest request) {
                return CompletableFuture.completedFuture(new VoteResponse(request.term(), peer.equals("n2")));
            }
            @Override public CompletableFuture<AppendResponse> appendEntries(String peer, String group, AppendRequest request) {
                if (!peer.equals("n2")) return unavailable();
                if (request.prevLogIndex() == 16) {
                    if (initialProbeRejected.compareAndSet(false, true)) {
                        return CompletableFuture.completedFuture(new AppendResponse(request.term(), false, 0, 1));
                    }
                    noopBlockedAfterOldBatch.countDown();
                    return unavailable();
                }
                if (request.entries().size() == 16) {
                    return CompletableFuture.completedFuture(new AppendResponse(request.term(), true, 16, 0));
                }
                return unavailable();
            }
        };
        try (RaftNode node = new RaftNode("n1", GROUP, MEMBERS, path, transport)) {
            node.start();
            assertTrue(noopBlockedAfterOldBatch.await(4, TimeUnit.SECONDS));
            assertEquals(0, node.status().commitIndex());
            assertEquals(0, node.status().keyCount());
        }
    }

    @Test
    void immutableBoundariesProtectStoredBytesAndGetNeverDeduplicates() throws Exception {
        try (Cluster cluster = new Cluster(directory)) {
            RaftNode leader = cluster.leader();
            byte[] bytes = "safe".getBytes(StandardCharsets.UTF_8);
            Command command = new Command(Command.Type.PUT, "bytes", bytes, "bytes-id");
            bytes[0] = 'X';
            command.value()[0] = 'X';
            await(leader.submit(command));
            Command sameReadId = new Command(Command.Type.GET, "bytes", null, "read-id");
            CommandResult result = await(leader.submit(sameReadId));
            result.value()[0] = 'X';
            assertEquals("safe", text(result.value()));
            await(leader.submit(put("bytes", "updated", "bytes-next")));
            assertEquals("updated", text(await(leader.submit(sameReadId)).value()));
            assertFalse(await(leader.submit(get("absent"))).found());
        }
    }

    @Test
    void diskErrorsFailClosedAndIdentityOrMembershipCannotChangeOnRestart() throws Exception {
        Path path = directory.resolve("one");
        try (RaftNode node = loneNode(path)) {
            // The temp path is a directory, so forcing the next vote to disk must fail.
            Files.createDirectory(path.resolve("raft-state.json.tmp"));
            assertFutureFailsWith(node.onRequestVote(new VoteRequest(10, "n2", 0, 0)), IllegalStateException.class);
            eventually(() -> node.status().role().equals("FAILED"), 1000);
            assertFutureFailsWith(node.onRequestVote(new VoteRequest(10, "n3", 0, 0)), IllegalStateException.class);
        }
        assertThrows(IOException.class, () -> new RaftNode("other", GROUP,
                List.of("n2", "n3"), path, disconnected()));
        assertThrows(IOException.class, () -> new RaftNode("n1", GROUP,
                List.of("n2", "n4"), path, disconnected()));
    }

    @Test
    void dataDirectoryCannotBeOpenedTwiceAndCorruptStateIsRejected() throws Exception {
        Path path = directory.resolve("one");
        try (RaftNode node = loneNode(path)) {
            assertThrows(RuntimeException.class, () -> new RaftNode("n1", GROUP, MEMBERS, path, disconnected()));
        }
        Files.writeString(path.resolve("raft-state.json"), "{broken}");
        assertThrows(IOException.class, () -> new RaftNode("n1", GROUP, MEMBERS, path, disconnected()));
    }

    private static Command put(String key, String value, String requestId) {
        return new Command(Command.Type.PUT, key, value.getBytes(StandardCharsets.UTF_8), requestId);
    }
    private static Command get(String key) { return new Command(Command.Type.GET, key, null, null); }
    private static String text(byte[] bytes) { return new String(bytes, StandardCharsets.UTF_8); }
    private static <T> T await(CompletableFuture<T> future) throws Exception { return future.get(6, TimeUnit.SECONDS); }
    private static void assertFutureFailsWith(CompletableFuture<?> future, Class<? extends Throwable> type) {
        ExecutionException failure = assertThrows(ExecutionException.class, () -> future.get(6, TimeUnit.SECONDS));
        assertInstanceOf(type, failure.getCause());
    }
    private static <T> CompletableFuture<T> unavailable() {
        return CompletableFuture.failedFuture(new IOException("simulated network partition"));
    }
    private static RaftTransport disconnected() {
        return new RaftTransport() {
            @Override public CompletableFuture<VoteResponse> requestVote(String p, String g, VoteRequest r) { return unavailable(); }
            @Override public CompletableFuture<AppendResponse> appendEntries(String p, String g, AppendRequest r) { return unavailable(); }
        };
    }
    private static RaftNode loneNode(Path path) throws IOException {
        RaftNode node = new RaftNode("n1", GROUP, MEMBERS, path, disconnected());
        node.start();
        return node;
    }
    private static void eventually(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(20);
        assertTrue(condition.getAsBoolean(), "Condition did not become true within " + timeoutMs + " ms");
    }

    private static final class Cluster implements AutoCloseable {
        private final Path directory;
        private final Map<String, RaftNode> nodes = new ConcurrentHashMap<>();
        private final Set<String> isolated = ConcurrentHashMap.newKeySet();

        Cluster(Path directory) throws IOException {
            this.directory = directory;
            for (String member : MEMBERS) create(member);
            nodes.values().forEach(RaftNode::start);
        }

        private void create(String id) throws IOException {
            RaftTransport transport = new RaftTransport() {
                private RaftNode target(String peer) {
                    return isolated.contains(id) || isolated.contains(peer) ? null : nodes.get(peer);
                }
                @Override public CompletableFuture<VoteResponse> requestVote(String peer, String group, VoteRequest request) {
                    RaftNode target = target(peer);
                    return target == null ? unavailable() : target.onRequestVote(request);
                }
                @Override public CompletableFuture<AppendResponse> appendEntries(String peer, String group, AppendRequest request) {
                    RaftNode target = target(peer);
                    return target == null ? unavailable() : target.onAppendEntries(request);
                }
                @Override public CompletableFuture<InstallSnapshotResponse> installSnapshot(String peer, String group,
                                                                                           InstallSnapshotRequest request) {
                    RaftNode target = target(peer);
                    return target == null ? unavailable() : target.onInstallSnapshot(request);
                }
            };
            nodes.put(id, new RaftNode(id, GROUP, MEMBERS, directory.resolve(id), transport));
        }

        RaftNode leader() throws InterruptedException { return leaderExcept(null); }
        RaftNode leaderExcept(String excluded) throws InterruptedException {
            RaftNode[] elected = new RaftNode[1];
            eventually(() -> {
                List<RaftNode> leaders = nodes.values().stream()
                        .filter(node -> !Objects.equals(node.status().nodeId(), excluded))
                        .filter(node -> node.status().role().equals("LEADER")).toList();
                if (leaders.size() != 1) return false;
                elected[0] = leaders.get(0);
                return true;
            }, 8000);
            return elected[0];
        }
        void isolate(String id) { isolated.add(id); }
        void heal() { isolated.clear(); }
        void stop(String id) { RaftNode node = nodes.remove(id); if (node != null) node.close(); }
        void restartAll() throws IOException {
            for (RaftNode node : nodes.values()) node.close();
            nodes.clear();
            for (String id : MEMBERS) create(id);
            nodes.values().forEach(RaftNode::start);
        }
        @Override public void close() { nodes.values().forEach(RaftNode::close); nodes.clear(); }
    }
}
