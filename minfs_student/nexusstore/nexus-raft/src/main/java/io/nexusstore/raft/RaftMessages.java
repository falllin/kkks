package io.nexusstore.raft;

import java.util.List;

public final class RaftMessages {
    private RaftMessages() { }

    public record LogEntry(long index, long term, Command command) { }
    public record VoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm) { }
    public record VoteResponse(long term, boolean voteGranted) { }
    public record AppendRequest(long term, String leaderId, long prevLogIndex,
                                long prevLogTerm, List<LogEntry> entries, long leaderCommit) {
        public AppendRequest {
            entries = entries == null ? null : List.copyOf(entries);
        }
    }
    public record AppendResponse(long term, boolean success, long matchIndex, long conflictIndex) { }
    /** Offsets count raw snapshot bytes, before JSON/base64 encoding. */
    public record InstallSnapshotRequest(long term, String leaderId, String snapshotId,
                                         long lastIncludedIndex, long lastIncludedTerm,
                                         long offset, byte[] data, boolean done,
                                         String checksum, long totalSize) {
        public InstallSnapshotRequest { data = data == null ? null : data.clone(); }
        @Override public byte[] data() { return data == null ? null : data.clone(); }
    }
    /** lastIncludedIndex is zero until the complete image is durably installed. */
    public record InstallSnapshotResponse(long term, boolean success, long nextOffset,
                                          long lastIncludedIndex) { }
}
