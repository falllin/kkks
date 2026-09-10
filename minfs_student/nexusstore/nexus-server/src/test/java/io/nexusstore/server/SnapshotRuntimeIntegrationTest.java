package io.nexusstore.server;

import io.nexusstore.raft.Command;
import io.nexusstore.raft.NodeStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.ObjectProvider;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/** Snapshot exceeds a single RPC frame: actual Netty must transfer it in bounded chunks. */
class SnapshotRuntimeIntegrationTest {
    @TempDir Path data;

    @Test void largeSnapshotCrossesNettyAndRecoveredReplicaParticipatesInNextQuorum() throws Exception {
        List<ServerSocket> reservations = new ArrayList<>();
        int[] ports = new int[3];
        try {
            for (int i = 0; i < 3; i++) { var socket = new ServerSocket(0); reservations.add(socket); ports[i] = socket.getLocalPort(); }
        } finally { for (ServerSocket socket : reservations) socket.close(); }
        String peers = "node1@127.0.0.1:" + ports[0] + ",node2@127.0.0.1:" + ports[1] + ",node3@127.0.0.1:" + ports[2];
        List<NodeRuntime> nodes = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) nodes.add(start(i, ports[i], peers));
            nodes.get(0).execute(put("initial", new byte[]{1}, "initial-request")).get(15, TimeUnit.SECONDS);
            NodeRuntime firstLeader = leader(nodes);
            int offline = nodes.indexOf(nodes.stream().filter(n -> n != firstLeader).findFirst().orElseThrow());
            long offlineApplied = state(nodes.get(offline)).lastApplied();
            nodes.get(offline).close();

            Map<String,byte[]> expected = new LinkedHashMap<>();
            for (int i = 0; i < 9; i++) {
                byte[] value = new byte[1024 * 1024];
                new Random(100 + i).nextBytes(value);
                String key = "snapshot-file-" + i;
                expected.put(key, value);
                firstLeader.execute(put(key, value, "large-write-" + i)).get(15, TimeUnit.SECONDS);
            }
            NodeRuntime snapshotLeader = leader(nodes);
            NodeStatus compacted = snapshotLeader.compactAll().get(15, TimeUnit.SECONDS).get(0);
            assertTrue(compacted.snapshotIndex() > offlineApplied);
            assertTrue(compacted.snapshotBytes() > 8L * 1024 * 1024, "image must exceed Netty's maximum frame size");
            assertEquals(0, compacted.retainedLogEntries(), "manual snapshot releases the applied prefix");

            nodes.set(offline, start(offline, ports[offline], peers));
            NodeRuntime recovered = nodes.get(offline);
            eventually(() -> state(recovered).snapshotIndex() >= compacted.snapshotIndex()
                    && state(recovered).lastApplied() >= compacted.snapshotIndex(), 30_000);

            // With the sender stopped, the installed snapshot replica is necessary for a quorum.
            snapshotLeader.close();
            for (var file : expected.entrySet()) {
                var result = recovered.execute(new Command(Command.Type.GET, file.getKey(), null, UUID.randomUUID().toString()))
                        .get(15, TimeUnit.SECONDS);
                assertArrayEquals(file.getValue(), result.value(), file.getKey());
            }
            byte[] replacement = new byte[]{8, 7, 6};
            recovered.execute(put("snapshot-file-0", replacement, "replacement")).get(15, TimeUnit.SECONDS);
            recovered.execute(put("snapshot-file-0", expected.get("snapshot-file-0"), "large-write-0")).get(15, TimeUnit.SECONDS);
            assertArrayEquals(replacement, recovered.execute(new Command(Command.Type.GET, "snapshot-file-0", null, "fresh-read"))
                    .get(15, TimeUnit.SECONDS).value(), "snapshot transfer retains old request deduplication");
        } finally { nodes.forEach(NodeRuntime::close); }
    }

    private NodeRuntime start(int index, int port, String peers) throws Exception {
        NexusProperties config = new NexusProperties();
        config.setNodeId("node" + (index + 1)); config.setRpcPort(port); config.setPeers(peers);
        config.setDataDir(data.resolve(config.getNodeId()).toString()); config.setShardCount(1);
        NodeRuntime runtime = new NodeRuntime(config, new PeerDirectory(config, mock(ObjectProvider.class)));
        runtime.start(); return runtime;
    }
    private static NodeStatus state(NodeRuntime node) { return node.statuses().get(0); }
    private static NodeRuntime leader(List<NodeRuntime> nodes) {
        return nodes.stream().filter(n -> state(n).role().equals("LEADER")).findFirst().orElseThrow();
    }
    private static Command put(String key, byte[] value, String id) { return new Command(Command.Type.PUT, key, value, id); }
    private static void eventually(BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(25);
        assertTrue(condition.getAsBoolean(), "replica did not install the snapshot before deadline");
    }
}
