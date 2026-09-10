package io.nexusstore.server;

import io.nexusstore.raft.Command;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.ObjectProvider;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/** Real Netty sockets + durable Raft, exercising the same routing used by HTTP. */
class NodeRuntimeIntegrationTest {
    @TempDir Path root;
    @Test void binaryFilesSurviveLeaderLossAndFullRestart() throws Exception {
        int[] ports = new int[3];
        for (int i = 0; i < 3; i++) try (ServerSocket socket = new ServerSocket(0)) { ports[i] = socket.getLocalPort(); }
        String peers = "node1@127.0.0.1:"+ports[0]+",node2@127.0.0.1:"+ports[1]+",node3@127.0.0.1:"+ports[2];
        List<NodeRuntime> nodes = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) nodes.add(start(i, ports[i], peers));
            byte[] payload = new byte[64 * 1024];
            new Random(23).nextBytes(payload);
            String key = "/binary/你好.bin";
            var written = nodes.get(0).execute(new Command(Command.Type.PUT, key, payload, "put-original")).get(15, TimeUnit.SECONDS);
            assertTrue(written.found());
            assertArrayEquals(payload, nodes.get(2).execute(read(key)).get(15, TimeUnit.SECONDS).value());
            String group = nodes.get(0).shardFor(key);
            NodeRuntime leader = nodes.stream().filter(n -> n.statuses().stream().anyMatch(s -> s.groupId().equals(group) && s.role().toString().equals("LEADER"))).findFirst().orElseThrow();
            int stopped = nodes.indexOf(leader);
            leader.close();
            NodeRuntime survivor = nodes.get((stopped + 1) % 3);
            assertArrayEquals(payload, survivor.execute(read(key)).get(15, TimeUnit.SECONDS).value());
            var retried = survivor.execute(new Command(Command.Type.PUT, key, payload, "put-original")).get(15, TimeUnit.SECONDS);
            assertEquals(written.index(), retried.index(), "write retry must return its original committed result");
            survivor.execute(new Command(Command.Type.PUT, "/after-failover", new byte[]{7,0,-1}, "after-failover")).get(15, TimeUnit.SECONDS);
            nodes.forEach(NodeRuntime::close);
            nodes.clear();
            for (int i = 0; i < 3; i++) nodes.add(start(i, ports[i], peers));
            assertArrayEquals(payload, nodes.get(1).execute(read(key)).get(15, TimeUnit.SECONDS).value());
            assertArrayEquals(new byte[]{7,0,-1}, nodes.get(2).execute(read("/after-failover")).get(15, TimeUnit.SECONDS).value());
            assertTrue(nodes.get(0).execute(new Command(Command.Type.DELETE, key, null, "delete-original")).get(15, TimeUnit.SECONDS).found());
            assertFalse(nodes.get(2).execute(read(key)).get(15, TimeUnit.SECONDS).found());
        } finally { nodes.forEach(NodeRuntime::close); }
    }
    private NodeRuntime start(int i, int port, String peers) throws Exception {
        NexusProperties config = new NexusProperties();
        config.setNodeId("node"+(i+1)); config.setRpcPort(port); config.setPeers(peers);
        config.setDataDir(root.resolve("node"+(i+1)).toString()); config.setShardCount(2);
        NodeRuntime runtime = new NodeRuntime(config, new PeerDirectory(config, mock(ObjectProvider.class)));
        runtime.start();
        return runtime;
    }
    private Command read(String key) { return new Command(Command.Type.GET, key, null, UUID.randomUUID().toString()); }
}
