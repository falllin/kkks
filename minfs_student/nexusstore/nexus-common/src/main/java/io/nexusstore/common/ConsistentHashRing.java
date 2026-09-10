package io.nexusstore.common;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

/** Immutable ring over logical shards; replica membership does not change file placement. */
public final class ConsistentHashRing {
    private final NavigableMap<Long, String> ring;

    public ConsistentHashRing() {
        this(4, 64);
    }

    public ConsistentHashRing(int shardCount, int virtualNodes) {
        if (shardCount < 1 || virtualNodes < 1 || (long) shardCount * virtualNodes > 1_000_000) {
            throw new IllegalArgumentException("Positive shard/virtual-node counts, at most 1000000 points required");
        }
        NavigableMap<Long, String> points = new TreeMap<>(Long::compareUnsigned);
        for (int shard = 0; shard < shardCount; shard++) {
            String id = "s" + shard;
            for (int vnode = 0; vnode < virtualNodes; vnode++) {
                points.put(hash(id + "#" + vnode), id);
            }
        }
        ring = java.util.Collections.unmodifiableNavigableMap(points);
    }

    public String shardFor(String key) {
        var successor = ring.ceilingEntry(hash(Objects.requireNonNull(key, "key")));
        return (successor == null ? ring.firstEntry() : successor).getValue();
    }

    /** First 64 SHA-256 bits in network byte order, compared as unsigned ring positions. */
    public static long hash(String value) {
        try {
            return ByteBuffer.wrap(MessageDigest.getInstance("SHA-256")
                    .digest(Objects.requireNonNull(value, "value").getBytes(StandardCharsets.UTF_8))).getLong();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("Java runtime lacks mandatory SHA-256", impossible);
        }
    }
}
