package io.nexusstore.raft;

/** A volatile, immutable snapshot; useful for diagnostics, never a read-consistency gate. */
public record NodeStatus(String nodeId, String groupId, String role, long term,
                         String leaderId, long commitIndex, long lastApplied,
                         long lastLogIndex, int keyCount, long snapshotIndex, long snapshotTerm,
                         int retainedLogEntries, long logBytes, long snapshotBytes) { }
