package io.nexusstore.server;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

/** Nacos discovers reachability. It must never change Raft membership or the hash ring. */
@Component
public class PeerDirectory {
    private static final Logger LOG = LoggerFactory.getLogger(PeerDirectory.class);
    private final Map<String,String> seeds;
    private final ObjectProvider<DiscoveryClient> discovery;
    private volatile Map<String,String> discovered = Map.of();
    public PeerDirectory(NexusProperties config, ObjectProvider<DiscoveryClient> discovery) {
        this.seeds = config.peerMap();
        this.discovery = discovery;
    }
    public String address(String nodeId) {
        String address = discovered.getOrDefault(nodeId, seeds.get(nodeId));
        if (address == null) throw new IllegalArgumentException("Unknown Raft peer: " + nodeId);
        return address;
    }
    public Map<String,String> seeds() { return seeds; }
    public Map<String,String> discovered() { return discovered; }
    @Scheduled(fixedDelay = 5000, initialDelay = 3000)
    public void refresh() {
        DiscoveryClient client = discovery.getIfAvailable();
        if (client == null) return;
        try {
            Map<String,String> next = new TreeMap<>();
            for (var instance : client.getInstances("nexus-store")) {
                String id = instance.getMetadata().get("node-id");
                String port = instance.getMetadata().get("netty-port");
                String endpoint = instance.getHost() + ":" + port;
                // Fixed topology for this demo; a discovery update is not a membership transaction.
                if (id != null && endpoint.equals(seeds.get(id))) next.put(id, endpoint);
            }
            discovered = Map.copyOf(next);
        } catch (RuntimeException e) {
            LOG.debug("Discovery unavailable; retaining fixed peers", e);
        }
    }
}
