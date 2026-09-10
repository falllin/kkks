package io.nexusstore.server;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.util.*;

@ConfigurationProperties("nexus")
public class NexusProperties {
    private String nodeId = "node1";
    private String host = "127.0.0.1";
    private int rpcPort = 9091;
    private String dataDir = "./data/node1";
    private String peers = "node1@127.0.0.1:9091,node2@127.0.0.1:9092,node3@127.0.0.1:9093";
    private int shardCount = 4;
    private int virtualNodes = 64;
    public Map<String,String> peerMap() {
        Map<String,String> result = new TreeMap<>();
        for (String item : peers.split(",")) {
            String[] pair = item.trim().split("@", -1);
            if (pair.length != 2 || !pair[0].matches("[a-zA-Z0-9_-]{1,32}") || !pair[1].matches("[a-zA-Z0-9.-]+:[0-9]{1,5}"))
                throw new IllegalArgumentException("peers must be nodeId@host:port comma separated");
            int port = Integer.parseInt(pair[1].substring(pair[1].lastIndexOf(':') + 1));
            if (port < 1 || port > 65535 || result.put(pair[0], pair[1]) != null)
                throw new IllegalArgumentException("Duplicate peer or invalid port");
        }
        if (result.size() != 3 || !result.containsKey(nodeId) || new HashSet<>(result.values()).size() != 3)
            throw new IllegalArgumentException("This demo requires exactly 3 distinct peers including nexus.node-id");
        if (shardCount < 1 || shardCount > 16 || virtualNodes < 1 || virtualNodes > 256)
            throw new IllegalArgumentException("shard-count: 1..16; virtual-nodes: 1..256");
        return Collections.unmodifiableMap(result);
    }
    public String getNodeId() { return nodeId; }
    public void setNodeId(String v) { nodeId = v; }
    public String getHost() { return host; }
    public void setHost(String v) { host = v; }
    public int getRpcPort() { return rpcPort; }
    public void setRpcPort(int v) { rpcPort = v; }
    public String getDataDir() { return dataDir; }
    public void setDataDir(String v) { dataDir = v; }
    public String getPeers() { return peers; }
    public void setPeers(String v) { peers = v; }
    public int getShardCount() { return shardCount; }
    public void setShardCount(int v) { shardCount = v; }
    public int getVirtualNodes() { return virtualNodes; }
    public void setVirtualNodes(int v) { virtualNodes = v; }
}
