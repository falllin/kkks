package io.nexusstore.raft;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/** Exercise observable recovery and Raft boundaries, including actual InstallSnapshot traffic. */
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class RaftSnapshotTest {
    @TempDir Path directory;
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final List<String> MEMBERS = List.of("n1", "n2", "n3");
    private static final String GROUP = "shard-0";

    @Test
    void repeatedCompactionKeepsAbsoluteIndexesAndMutationResults() throws Exception {
        try (Cluster cluster = new Cluster(directory)) {
            RaftNode leader = cluster.leader();
            Command original = put("file", "first", "original");
            CommandResult originalResult = await(leader.submit(original));
            Command deletion = new Command(Command.Type.DELETE, "file", null, "delete-once");
            CommandResult deleteResult = await(leader.submit(deletion));
            assertTrue(deleteResult.found());
            await(leader.submit(put("file", "current", "replace")));
            Command absentDeletion = new Command(Command.Type.DELETE, "missing", null, "delete-missing");
            CommandResult absentResult = await(leader.submit(absentDeletion));
            assertFalse(absentResult.found());
            NodeStatus first = await(leader.compact());
            assertTrue(first.snapshotIndex() >= absentResult.index());
            assertEquals(first.lastApplied(), first.snapshotIndex());
            assertEquals(0, first.retainedLogEntries());
            assertEquals(0, first.logBytes());
            assertTrue(first.snapshotBytes() > 0);

            assertEquals(originalResult.index(), await(leader.submit(original)).index());
            assertEquals(deleteResult.index(), await(leader.submit(deletion)).index());
            assertEquals(absentResult.index(), await(leader.submit(absentDeletion)).index());
            assertEquals("current", text(await(leader.submit(get("file"))).value()));
            assertFailure(leader.submit(put("file", "conflicting", "original")), IllegalArgumentException.class);

            CommandResult next = await(leader.submit(put("next", "next-value", "next")));
            assertTrue(next.index() > first.snapshotIndex());
            NodeStatus second = await(leader.compact());
            assertTrue(second.snapshotIndex() > first.snapshotIndex());
            assertEquals(second.snapshotIndex(), second.lastLogIndex());
            assertEquals(second.snapshotIndex(), second.commitIndex());
            assertEquals("next-value", text(await(leader.submit(get("next"))).value()));
        }
    }

    @Test
    void laggingFollowerReceivesChunkedSnapshotRetriesLostAcknowledgementAndCanLead() throws Exception {
        try (Cluster cluster = new Cluster(directory)) {
            RaftNode leader = cluster.leader();
            String laggingId = cluster.otherThan(leader.status().nodeId());
            cluster.stop(laggingId);
            byte[] large = new byte[640 * 1024];
            new Random(17).nextBytes(large);
            Command original = new Command(Command.Type.PUT, "large", large, "large-id");
            CommandResult originalResult = await(leader.submit(original));
            for (int i = 0; i < 18; i++) await(leader.submit(put("counter", "v" + i, "id-" + i)));
            await(leader.submit(put("deleted", "old", "before-delete")));
            Command deletion = new Command(Command.Type.DELETE, "deleted", null, "delete-id");
            long deleteIndex = await(leader.submit(deletion)).index();
            long boundary = await(leader.compact()).snapshotIndex();
            long tailIndex = await(leader.submit(put("tail", "after-snapshot", "tail-id"))).index();

            cluster.dropSnapshotAcknowledgement.set(true);
            RaftNode recovered = cluster.restart(laggingId);
            eventually(() -> recovered.status().snapshotIndex() >= boundary
                    && recovered.status().lastApplied() >= tailIndex, 10_000);
            assertTrue(cluster.deliveredSnapshotChunks.get() >= 4,
                    "640 KiB must use multiple <= 256 KiB chunks plus a retry");
            assertEquals(1, cluster.lostSnapshotAcknowledgements.get());
            assertTrue(cluster.snapshotOffsets.stream().filter(offset -> offset == 0).count() >= 2,
                    "Lost acknowledgement must cause the first chunk to be retransmitted");
            assertTrue(recovered.status().commitIndex() >= tailIndex);

            // The recovered follower itself must serve the values after winning an election.
            cluster.allowedCandidate = laggingId;
            cluster.stop(leader.status().nodeId());
            RaftNode newLeader = cluster.leader();
            assertEquals(laggingId, newLeader.status().nodeId());
            assertArrayEquals(large, await(newLeader.submit(get("large"))).value());
            assertEquals("v17", text(await(newLeader.submit(get("counter"))).value()));
            assertEquals("after-snapshot", text(await(newLeader.submit(get("tail"))).value()));
            assertFalse(await(newLeader.submit(get("deleted"))).found());
            assertEquals(originalResult.index(), await(newLeader.submit(original)).index());
            assertEquals(deleteIndex, await(newLeader.submit(deletion)).index());
        }
    }

    @Test
    void snapshotsAndRetainedTailSurviveLeaderFailureAndEntireClusterRestart() throws Exception {
        try (Cluster cluster = new Cluster(directory)) {
            RaftNode leader = cluster.leader();
            Command before = put("before", "snapshot-value", "before-id");
            long beforeIndex = await(leader.submit(before)).index();
            cluster.compactAllAfter(beforeIndex);
            Command tail = put("tail", "tail-value", "tail-id");
            long tailIndex = await(leader.submit(tail)).index();
            cluster.appliedEverywhere(tailIndex);
            cluster.stop(leader.status().nodeId());
            leader = cluster.leader();
            assertEquals("snapshot-value", text(await(leader.submit(get("before"))).value()));
            assertEquals("tail-value", text(await(leader.submit(get("tail"))).value()));
            cluster.restartAll();
            for (RaftNode node : cluster.nodes.values()) {
                assertTrue(node.status().snapshotIndex() >= beforeIndex);
                assertTrue(node.status().lastApplied() >= tailIndex);
            }
            leader = cluster.leader();
            assertEquals("snapshot-value", text(await(leader.submit(get("before"))).value()));
            assertEquals("tail-value", text(await(leader.submit(get("tail"))).value()));
            assertEquals(beforeIndex, await(leader.submit(before)).index());
            assertEquals(tailIndex, await(leader.submit(tail)).index());
        }
    }

    @Test
    void automaticCompactionRunsPastOldLogCapacityAndRestoresPropertyValues() throws Exception {
        try (PropertiesScope ignored = new PropertiesScope(Map.of(
                "nexus.raft.maxLogEntries", "24", "nexus.raft.snapshotEntries", "8"));
             Cluster cluster = new Cluster(directory)) {
            RaftNode leader = cluster.leader();
            long last = 0;
            for (int i = 0; i < 80; i++) {
                last = await(leader.submit(put("counter", "value-" + i, "request-" + i))).index();
            }
            assertTrue(last > 24, "Absolute indexes must continue beyond the retained-log limit");
            long finalIndex = last;
            eventually(() -> leader.status().snapshotIndex() >= finalIndex - 7, 2000);
            assertTrue(leader.status().retainedLogEntries() < 8);
            assertEquals("value-79", text(await(leader.submit(get("counter"))).value()));
            cluster.restartAll();
            assertEquals("value-79", text(await(cluster.leader().submit(get("counter"))).value()));
        }
    }

    @Test
    void automaticCompactionAlsoUsesBytesThreshold() throws Exception {
        try (PropertiesScope ignored = new PropertiesScope(Map.of(
                "nexus.raft.snapshotEntries", "1000", "nexus.raft.snapshotLogBytes", "1024"));
             Cluster cluster = new Cluster(directory)) {
            RaftNode leader = cluster.leader();
            long lastIndex = 0;
            for (int i = 0; i < 6; i++) {
                byte[] value = new byte[900];
                Arrays.fill(value, (byte) i);
                lastIndex = await(leader.submit(new Command(Command.Type.PUT, "bytes", value, "byte-id-" + i))).index();
            }
            // Submission completes when applied, before compaction/status publication finishes.
            // Each 900-byte PUT plus its entry overhead exceeds the 1024-byte threshold.
            long finalIndex = lastIndex;
            eventually(() -> leader.status().snapshotIndex() >= finalIndex, 2000);
            assertTrue(leader.status().lastLogIndex() < 1000);
            assertTrue(leader.status().logBytes() < 1024);
            assertEquals(5, await(leader.submit(get("bytes"))).value()[0]);
        }
    }

    @Test
    void fullUncommittedFollowerLogCanCompactVerifiedPrefixBeforeAcceptingNextEntry() throws Exception {
        Path path = directory.resolve("full-tail");
        try (PropertiesScope ignored = new PropertiesScope(Map.of(
                "nexus.raft.maxLogEntries", "24", "nexus.raft.snapshotEntries", "1000",
                "nexus.raft.snapshotLogBytes", "1048576"))) {
            try (RaftNode node = startNode(path, disconnected())) {
                List<LogEntry> entries = new ArrayList<>();
                for (int index = 1; index <= 25; index++) {
                    entries.add(new LogEntry(index, 1, put("key-" + index, "value-" + index, "id-" + index)));
                }
                assertTrue(await(node.onAppendEntries(new AppendRequest(1, "n2", 0, 0,
                        entries.subList(0, 16), 0))).success());
                assertTrue(await(node.onAppendEntries(new AppendRequest(1, "n2", 16, 1,
                        entries.subList(16, 24), 0))).success());
                eventually(() -> node.status().lastLogIndex() == 24, 1000);
                assertEquals(0, node.status().commitIndex());
                assertEquals(0, node.status().snapshotIndex());
                assertEquals(24, node.status().retainedLogEntries());

                // prevLogIndex/prevLogTerm prove the existing prefix. The incoming entry is not committed.
                AppendResponse accepted = await(node.onAppendEntries(new AppendRequest(1, "n2", 24, 1,
                        List.of(entries.get(24)), 24)));
                assertTrue(accepted.success());
                assertEquals(25, accepted.matchIndex());
                eventually(() -> node.status().snapshotIndex() == 24, 1000);
                assertEquals(24, node.status().commitIndex());
                assertEquals(24, node.status().lastApplied());
                assertEquals(25, node.status().lastLogIndex());
                assertEquals(1, node.status().retainedLogEntries());
                assertEquals(24, node.status().keyCount(), "Entry 25 must remain unapplied until its commit is advertised");

                assertTrue(await(node.onAppendEntries(new AppendRequest(1, "n2", 25, 1, List.of(), 25))).success());
                eventually(() -> node.status().lastApplied() == 25, 1000);
                assertEquals(25, node.status().keyCount());
            }
            try (RaftNode recovered = startNode(path, grantingTransport())) {
                assertEquals(24, recovered.status().snapshotIndex());
                assertEquals(25, recovered.status().lastLogIndex());
                assertEquals(25, recovered.status().lastApplied());
                eventually(() -> recovered.status().role().equals("LEADER"), 8000);
                assertEquals("value-1", text(await(recovered.submit(get("key-1"))).value()));
                assertEquals("value-25", text(await(recovered.submit(get("key-25"))).value()));
            }
        }
    }

    @Test
    void fullRequestHistoryRejectsNewMutationsButAllowsOldRetriesAndReads() throws Exception {
        try (PropertiesScope ignored = new PropertiesScope(Map.of(
                "nexus.raft.maxStateEntries", "3", "nexus.raft.snapshotEntries", "1"));
             Cluster cluster = new Cluster(directory)) {
            RaftNode leader = cluster.leader();
            Command original = put("file", "first", "first-id");
            long originalIndex = await(leader.submit(original)).index();
            await(leader.submit(put("file", "second", "second-id")));
            long finalIndex = await(leader.submit(put("file", "third", "third-id"))).index();
            eventually(() -> leader.status().snapshotIndex() >= finalIndex, 2000);
            assertFailure(leader.submit(put("file", "over-capacity", "fourth-id")), RejectedExecutionException.class);
            assertEquals(originalIndex, await(leader.submit(original)).index());
            assertEquals("third", text(await(leader.submit(get("file"))).value()));
            assertEquals("LEADER", leader.status().role());
        }
    }

    @Test
    void snapshotByteLimitIncludesUncommittedMutationsWhenAdmittingConcurrentWrites() throws Exception {
        try (PropertiesScope ignored = new PropertiesScope(Map.of(
                "nexus.raft.maxSnapshotBytes", "4096", "nexus.raft.snapshotEntries", "1"));
             Cluster cluster = new Cluster(directory)) {
            RaftNode leader = cluster.leader();
            long baseIndex = await(leader.submit(new Command(Command.Type.PUT, "base", new byte[2600], "base-id"))).index();
            eventually(() -> leader.status().snapshotIndex() >= baseIndex, 2000);
            cluster.isolate(leader.status().nodeId());
            CompletableFuture<CommandResult> pending = leader.submit(
                    new Command(Command.Type.PUT, "pending-1", new byte[800], "pending-id-1"));
            CompletableFuture<CommandResult> excess = leader.submit(
                    new Command(Command.Type.PUT, "pending-2", new byte[800], "pending-id-2"));
            assertFailure(excess, RejectedExecutionException.class);
            eventually(() -> leader.status().lastLogIndex() == baseIndex + 1, 1000);
            assertFalse(pending.isDone(), "The first write fits but cannot commit without a majority");
            assertEquals(baseIndex, leader.status().commitIndex());
            assertEquals("LEADER", leader.status().role());
            assertEquals(1, leader.status().keyCount());
        }
    }

    @Test
    void compactedRequestHistoryKeepsFingerprintsInsteadOfAllOverwrittenFileBodies() throws Exception {
        try (Cluster cluster = new Cluster(directory)) {
            RaftNode leader = cluster.leader();
            Command first = null;
            long firstIndex = 0;
            byte[] finalValue = null;
            for (int i = 0; i < 12; i++) {
                byte[] value = new byte[256 * 1024];
                new Random(i).nextBytes(value);
                Command command = new Command(Command.Type.PUT, "overwritten", value, "large-" + i);
                long index = await(leader.submit(command)).index();
                if (first == null) { first = command; firstIndex = index; }
                finalValue = value;
            }
            NodeStatus compacted = await(leader.compact());
            assertTrue(compacted.snapshotBytes() < 320 * 1024,
                    "Only the current 256 KiB file plus bounded fingerprints should remain");
            assertEquals(firstIndex, await(leader.submit(first)).index());
            assertArrayEquals(finalValue, await(leader.submit(get("overwritten"))).value());
            assertFailure(leader.submit(put("overwritten", "different", "large-0")), IllegalArgumentException.class);
        }
    }

    @Test
    void formatOneStateMigratesWithCommittedValuesDeduplicationAndUncommittedTail() throws Exception {
        Path path = directory.resolve("legacy");
        Files.createDirectories(path);
        Files.writeString(path.resolve("raft-state.json"), """
                {"formatVersion":1,"nodeId":"n1","groupId":"shard-0","members":["n1","n2","n3"],
                 "term":5,"votedFor":"n2","commitIndex":3,"log":[
                  {"index":1,"term":3,"command":{"type":"PUT","key":"kept","value":"b2xk","requestId":"old-put"}},
                  {"index":2,"term":3,"command":{"type":"DELETE","key":"kept","value":null,"requestId":"old-delete"}},
                  {"index":3,"term":4,"command":{"type":"PUT","key":"kept","value":"bmV3","requestId":"new-put"}},
                  {"index":4,"term":5,"command":{"type":"PUT","key":"tail","value":"dGFpbA==","requestId":"tail-put"}}
                 ]}
                """);
        try (RaftNode node = startNode(path, disconnected())) {
            assertEquals(3, node.status().lastApplied());
            assertEquals(4, node.status().lastLogIndex());
            assertEquals(1, node.status().keyCount());
            assertFalse(await(node.onRequestVote(new VoteRequest(5, "n3", 4, 5))).voteGranted());
            NodeStatus compacted = await(node.compact());
            assertEquals(3, compacted.snapshotIndex());
            assertEquals(4, compacted.snapshotTerm());
            assertEquals(1, compacted.retainedLogEntries());
            assertTrue(await(node.onAppendEntries(new AppendRequest(5, "n2", 4, 5, List.of(), 4))).success());
        }
        try (RaftNode restarted = startNode(path, grantingTransport())) {
            eventually(() -> restarted.status().role().equals("LEADER"), 8000);
            assertEquals("new", text(await(restarted.submit(get("kept"))).value()));
            assertEquals("tail", text(await(restarted.submit(get("tail"))).value()));
            assertEquals(1, await(restarted.submit(put("kept", "old", "old-put"))).index());
            CommandResult deletion = await(restarted.submit(new Command(Command.Type.DELETE, "kept", null, "old-delete")));
            assertTrue(deletion.found());
            assertEquals(2, deletion.index());
            assertEquals("new", text(await(restarted.submit(get("kept"))).value()));
        }
        assertEquals(2, JSON.readTree(path.resolve("raft-state.json").toFile()).path("formatVersion").asInt());
    }

    @Test
    void snapshotInstallKeepsOnlyTailWithMatchingBoundaryTerm() throws Exception {
        byte[] matching = image(2, 1, Map.of("a", bytes("one"), "b", bytes("two")));
        try (RaftNode node = startNode(directory.resolve("matching"), disconnected())) {
            seedUncommittedTail(node);
            assertTrue(await(node.onInstallSnapshot(chunk(matching, 3, 2, 1, "matching", 0, matching.length))).success());
            eventually(() -> node.status().snapshotIndex() == 2, 1000);
            assertEquals(3, node.status().lastLogIndex());
            assertEquals(1, node.status().retainedLogEntries());
            assertEquals(2, node.status().commitIndex());
            assertEquals(2, node.status().keyCount(), "Uncommitted local tail must stay unapplied");
            assertTrue(await(node.onAppendEntries(new AppendRequest(3, "n2", 3, 2, List.of(), 3))).success());
            eventually(() -> node.status().lastApplied() == 3, 1000);
            assertEquals(3, node.status().keyCount());
        }
        byte[] conflicting = image(2, 2, Map.of("a", bytes("one"), "b", bytes("replacement")));
        try (RaftNode node = startNode(directory.resolve("conflicting"), disconnected())) {
            seedUncommittedTail(node);
            assertTrue(await(node.onInstallSnapshot(chunk(conflicting, 3, 2, 2, "conflicting", 0, conflicting.length))).success());
            eventually(() -> node.status().snapshotIndex() == 2, 1000);
            assertEquals(2, node.status().lastLogIndex());
            assertEquals(0, node.status().retainedLogEntries());
            assertEquals(2, node.status().commitIndex());
            assertFalse(await(node.onAppendEntries(new AppendRequest(3, "n2", 3, 2, List.of(), 3))).success());
            assertEquals(2, node.status().keyCount());
        }
    }

    @Test
    void staleSnapshotsCannotRollBackCommittedStateOrTerm() throws Exception {
        Path path = directory.resolve("stale");
        try (RaftNode node = startNode(path, disconnected())) {
            List<LogEntry> committed = List.of(new LogEntry(1, 1, put("a", "one", "a")),
                    new LogEntry(2, 2, put("b", "two", "b")));
            await(node.onAppendEntries(new AppendRequest(3, "n2", 0, 0, committed, 2)));
            await(node.compact());
            byte[] stale = image(1, 1, Map.of("wrong", bytes("stale")));
            await(node.onInstallSnapshot(chunk(stale, 3, 1, 1, "stale-index", 0, stale.length)));
            InstallSnapshotResponse oldTerm = await(node.onInstallSnapshot(chunk(stale, 2, 1, 1, "stale-term", 0, stale.length)));
            assertFalse(oldTerm.success());
            assertEquals(3, oldTerm.term());
            assertEquals(2, node.status().snapshotIndex());
            assertEquals(2, node.status().commitIndex());
            assertEquals(2, node.status().lastApplied());
            assertEquals(2, node.status().keyCount());
        }
        try (RaftNode reopened = startNode(path, grantingTransport())) {
            eventually(() -> reopened.status().role().equals("LEADER"), 8000);
            assertEquals("one", text(await(reopened.submit(get("a"))).value()));
            assertEquals("two", text(await(reopened.submit(get("b"))).value()));
            assertFalse(await(reopened.submit(get("wrong"))).found());
        }
    }

    @Test
    void invalidChecksumAndOffsetAreRejectedWithoutApplyingPartialState() throws Exception {
        byte[] payload = image(4, 2, Map.of("file", new byte[320 * 1024]));
        try (RaftNode node = startNode(directory.resolve("validation"), disconnected())) {
            assertRejected(node.onInstallSnapshot(chunk(payload, 3, 4, 2, "out-of-order", 128, 256)));
            assertEquals(0, node.status().snapshotIndex());
            assertTrue(await(node.onInstallSnapshot(chunk(payload, 3, 4, 2, "checksum", 0, 256 * 1024))).success());
            assertEquals(0, node.status().lastApplied());
            byte[] badEnd = Arrays.copyOfRange(payload, 256 * 1024, payload.length);
            badEnd[badEnd.length - 1] ^= 1;
            InstallSnapshotRequest invalid = new InstallSnapshotRequest(3, "n2", "checksum", 4, 2,
                    256 * 1024, badEnd, true, SnapshotCodec.checksum(payload), payload.length);
            assertRejected(node.onInstallSnapshot(invalid));
            assertEquals(0, node.status().commitIndex());
            assertEquals(0, node.status().snapshotIndex());
            assertNotEquals("FAILED", node.status().role());
            installAll(node, payload, 3, 4, 2, "clean-retry");
            eventually(() -> node.status().snapshotIndex() == 4, 1000);
            assertEquals(1, node.status().keyCount());
        }
    }

    @Test
    void processRestartDuringTransferRequiresFreshPrefixAndNeverExposesPartialImage() throws Exception {
        byte[] payload = image(4, 2, Map.of("file", new byte[320 * 1024]));
        Path path = directory.resolve("interrupted");
        try (RaftNode node = startNode(path, disconnected())) {
            assertTrue(await(node.onInstallSnapshot(chunk(payload, 3, 4, 2, "interrupted", 0, 128 * 1024))).success());
            assertEquals(0, node.status().snapshotIndex());
            assertEquals(0, node.status().keyCount());
        }
        try (RaftNode node = startNode(path, disconnected())) {
            assertEquals(0, node.status().keyCount());
            InstallSnapshotResponse resumed = await(node.onInstallSnapshot(
                    chunk(payload, 3, 4, 2, "interrupted", 128 * 1024, 256 * 1024)));
            assertFalse(resumed.success());
            assertEquals(0, resumed.nextOffset());
            installAll(node, payload, 3, 4, 2, "interrupted");
            eventually(() -> node.status().lastApplied() == 4, 1000);
        }
        try (RaftNode node = startNode(path, disconnected())) {
            assertEquals(4, node.status().snapshotIndex());
            assertEquals(4, node.status().lastApplied());
            assertEquals(1, node.status().keyCount());
        }
    }

    @Test
    void corruptPersistedSnapshotIsRejectedAtStartup() throws Exception {
        Path path = directory.resolve("corrupt");
        try (RaftNode node = startNode(path, disconnected())) {
            await(node.onAppendEntries(new AppendRequest(2, "n2", 0, 0,
                    List.of(new LogEntry(1, 2, put("file", "durable", "write"))), 1)));
            await(node.compact());
        }
        Path stateFile = path.resolve("raft-state.json");
        ObjectNode state = (ObjectNode) JSON.readTree(stateFile.toFile());
        ObjectNode snapshot = (ObjectNode) state.path("snapshot");
        byte[] corrupt = snapshot.path("data").binaryValue();
        corrupt[corrupt.length - 1] ^= 1;
        snapshot.put("data", corrupt);
        JSON.writeValue(stateFile.toFile(), state);
        assertThrows(IOException.class, () -> new RaftNode("n1", GROUP, MEMBERS, path, disconnected()));
    }

    private static void seedUncommittedTail(RaftNode node) throws Exception {
        List<LogEntry> entries = List.of(new LogEntry(1, 1, put("a", "one", "a")),
                new LogEntry(2, 1, put("b", "two", "b")), new LogEntry(3, 2, put("c", "tail", "c")));
        assertTrue(await(node.onAppendEntries(new AppendRequest(3, "n2", 0, 0, entries, 1))).success());
    }
    private static byte[] image(long index, long term, Map<String, byte[]> values) throws IOException {
        return SnapshotCodec.encode(new SnapshotImage(GROUP, MEMBERS, index, term, values, Map.of()), 1024 * 1024);
    }
    private static InstallSnapshotRequest chunk(byte[] payload, long term, long index, long snapshotTerm,
                                               String id, int from, int to) {
        return new InstallSnapshotRequest(term, "n2", id, index, snapshotTerm, from,
                Arrays.copyOfRange(payload, from, to), to == payload.length,
                SnapshotCodec.checksum(payload), payload.length);
    }
    private static void installAll(RaftNode node, byte[] payload, long term, long index,
                                   long snapshotTerm, String id) throws Exception {
        for (int offset = 0; offset < payload.length; offset += 256 * 1024) {
            int end = Math.min(payload.length, offset + 256 * 1024);
            InstallSnapshotResponse response = await(node.onInstallSnapshot(chunk(payload, term, index, snapshotTerm, id, offset, end)));
            assertTrue(response.success());
            assertEquals(end, response.nextOffset());
        }
    }
    private static void assertRejected(CompletableFuture<InstallSnapshotResponse> result) throws Exception {
        try { assertFalse(await(result).success()); }
        catch (ExecutionException rejected) { assertInstanceOf(IllegalArgumentException.class, rejected.getCause()); }
    }
    private static Command put(String key, String value, String id) { return new Command(Command.Type.PUT, key, bytes(value), id); }
    private static Command get(String key) { return new Command(Command.Type.GET, key, null, null); }
    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private static String text(byte[] value) { return new String(value, StandardCharsets.UTF_8); }
    private static <T> T await(CompletableFuture<T> result) throws Exception { return result.get(8, TimeUnit.SECONDS); }
    private static void assertFailure(CompletableFuture<?> result, Class<? extends Throwable> cause) {
        ExecutionException failure = assertThrows(ExecutionException.class, () -> result.get(8, TimeUnit.SECONDS));
        assertInstanceOf(cause, failure.getCause());
    }
    private static <T> CompletableFuture<T> unavailable() { return CompletableFuture.failedFuture(new IOException("simulated outage")); }
    private static RaftTransport disconnected() {
        return new RaftTransport() {
            @Override public CompletableFuture<VoteResponse> requestVote(String peer, String group, VoteRequest r) { return unavailable(); }
            @Override public CompletableFuture<AppendResponse> appendEntries(String peer, String group, AppendRequest r) { return unavailable(); }
        };
    }
    /** Only for deterministic persisted-state tests; actual replication is tested by Cluster below. */
    private static RaftTransport grantingTransport() {
        return new RaftTransport() {
            @Override public CompletableFuture<VoteResponse> requestVote(String peer, String group, VoteRequest r) {
                return CompletableFuture.completedFuture(new VoteResponse(r.term(), true));
            }
            @Override public CompletableFuture<AppendResponse> appendEntries(String peer, String group, AppendRequest r) {
                return CompletableFuture.completedFuture(new AppendResponse(r.term(), true, r.prevLogIndex() + r.entries().size(), 0));
            }
        };
    }
    private static RaftNode startNode(Path path, RaftTransport transport) throws IOException {
        RaftNode node = new RaftNode("n1", GROUP, MEMBERS, path, transport);
        node.start(); return node;
    }
    private static void eventually(BooleanSupplier condition, long timeout) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(20);
        assertTrue(condition.getAsBoolean(), "Condition did not become true within " + timeout + " ms");
    }

    private static final class PropertiesScope implements AutoCloseable {
        private final Map<String, String> previous = new HashMap<>();
        PropertiesScope(Map<String, String> values) {
            values.forEach((key, value) -> previous.put(key, System.setProperty(key, value)));
        }
        @Override public void close() {
            previous.forEach((key, value) -> { if (value == null) System.clearProperty(key); else System.setProperty(key, value); });
        }
    }

    private static final class Cluster implements AutoCloseable {
        private final Path directory;
        private final Map<String, RaftNode> nodes = new ConcurrentHashMap<>();
        private final Set<String> isolated = ConcurrentHashMap.newKeySet();
        final AtomicInteger deliveredSnapshotChunks = new AtomicInteger();
        final AtomicInteger lostSnapshotAcknowledgements = new AtomicInteger();
        final AtomicBoolean dropSnapshotAcknowledgement = new AtomicBoolean();
        final Queue<Long> snapshotOffsets = new ConcurrentLinkedQueue<>();
        volatile String allowedCandidate;

        Cluster(Path directory) throws IOException {
            this.directory = directory;
            for (String member : MEMBERS) create(member);
            nodes.values().forEach(RaftNode::start);
        }
        private RaftNode create(String id) throws IOException {
            RaftTransport transport = new RaftTransport() {
                private RaftNode target(String peer) {
                    return isolated.contains(id) || isolated.contains(peer) ? null : nodes.get(peer);
                }
                @Override public CompletableFuture<VoteResponse> requestVote(String peer, String group, VoteRequest request) {
                    RaftNode node = target(peer);
                    if (node == null) return unavailable();
                    if (allowedCandidate != null && !allowedCandidate.equals(request.candidateId())) {
                        return CompletableFuture.completedFuture(new VoteResponse(node.status().term(), false));
                    }
                    return node.onRequestVote(request);
                }
                @Override public CompletableFuture<AppendResponse> appendEntries(String peer, String group, AppendRequest request) {
                    RaftNode node = target(peer);
                    return node == null ? unavailable() : node.onAppendEntries(request);
                }
                @Override public CompletableFuture<InstallSnapshotResponse> installSnapshot(String peer, String group,
                                                                                           InstallSnapshotRequest request) {
                    RaftNode node = target(peer);
                    if (node == null) return unavailable();
                    deliveredSnapshotChunks.incrementAndGet();
                    snapshotOffsets.add(request.offset());
                    CompletableFuture<InstallSnapshotResponse> received = node.onInstallSnapshot(request);
                    if (dropSnapshotAcknowledgement.compareAndSet(true, false)) {
                        lostSnapshotAcknowledgements.incrementAndGet();
                        return received.thenCompose(ignored -> unavailable());
                    }
                    return received;
                }
            };
            RaftNode node = new RaftNode(id, GROUP, MEMBERS, directory.resolve(id), transport);
            nodes.put(id, node); return node;
        }
        String otherThan(String id) { return MEMBERS.stream().filter(member -> !member.equals(id)).findFirst().orElseThrow(); }
        RaftNode leader() throws InterruptedException {
            RaftNode[] selected = new RaftNode[1];
            eventually(() -> {
                List<RaftNode> leaders = nodes.values().stream().filter(node -> node.status().role().equals("LEADER")).toList();
                if (leaders.size() != 1) return false;
                selected[0] = leaders.get(0); return true;
            }, 8000);
            return selected[0];
        }
        void appliedEverywhere(long index) throws InterruptedException {
            eventually(() -> nodes.values().stream().allMatch(node -> node.status().lastApplied() >= index), 5000);
        }
        void compactAllAfter(long index) throws Exception {
            appliedEverywhere(index);
            for (RaftNode node : nodes.values()) assertTrue(await(node.compact()).snapshotIndex() >= index);
        }
        void stop(String id) { RaftNode node = nodes.remove(id); if (node != null) node.close(); }
        void isolate(String id) { isolated.add(id); }
        RaftNode restart(String id) throws IOException { RaftNode node = create(id); node.start(); return node; }
        void restartAll() throws IOException {
            nodes.values().forEach(RaftNode::close); nodes.clear();
            for (String member : MEMBERS) create(member);
            nodes.values().forEach(RaftNode::start);
        }
        @Override public void close() { nodes.values().forEach(RaftNode::close); nodes.clear(); }
    }
}
