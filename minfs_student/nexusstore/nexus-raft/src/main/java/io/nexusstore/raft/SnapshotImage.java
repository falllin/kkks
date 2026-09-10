package io.nexusstore.raft;

import java.util.List;
import java.util.Map;

/** Internal state-machine image; no node identity, so any member can install it. */
record SnapshotImage(String groupId, List<String> members, long index, long term,
                     Map<String, byte[]> values, Map<String, AppliedRequest> requests) {
    /** Retain only a command fingerprint and its original mutation result, never old PUT bytes. */
    record AppliedRequest(String fingerprint, CommandResult result) { }
}
