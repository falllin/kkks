package io.nexusstore.common;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConsistentHashRingTest {
    @Test void deterministicAcrossInstancesAndUtf8() {
        ConsistentHashRing first = new ConsistentHashRing(4, 64);
        ConsistentHashRing second = new ConsistentHashRing();
        assertEquals(0xba7816bf8f01cfeaL, ConsistentHashRing.hash("abc"));
        for (int i = 0; i < 1000; i++) {
            String key = "目录/文件-" + i;
            assertEquals(first.shardFor(key), second.shardFor(key));
        }
    }

    @Test void virtualNodesSpreadKeysAndAddingShardOnlyMovesKeysToNewShard() {
        ConsistentHashRing four = new ConsistentHashRing(4, 64);
        ConsistentHashRing five = new ConsistentHashRing(5, 64);
        Map<String, Integer> counts = new HashMap<>();
        int moved = 0;
        for (int i = 0; i < 20_000; i++) {
            String key = "file/" + i;
            String before = four.shardFor(key);
            String after = five.shardFor(key);
            counts.merge(before, 1, Integer::sum);
            if (!before.equals(after)) {
                assertEquals("s4", after);
                moved++;
            }
        }
        assertEquals(4, counts.size());
        counts.forEach((shard, count) -> assertTrue(count > 3000 && count < 7000, counts.toString()));
        assertTrue(moved > 2000 && moved < 6500, "moved=" + moved);
    }

    @Test void rejectsInvalidConfiguration() {
        assertThrows(IllegalArgumentException.class, () -> new ConsistentHashRing(0, 64));
        assertThrows(IllegalArgumentException.class, () -> new ConsistentHashRing(4, 0));
        assertThrows(IllegalArgumentException.class, () -> new ConsistentHashRing(Integer.MAX_VALUE, 64));
        assertEquals("s0", new ConsistentHashRing(1, 1).shardFor("anything"));
    }
}
