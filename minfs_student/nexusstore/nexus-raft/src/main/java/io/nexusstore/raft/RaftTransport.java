package io.nexusstore.raft;

import java.util.concurrent.CompletableFuture;
import io.nexusstore.raft.RaftMessages.*;

/** Implementations must return promptly; RPC waiting must never block a Raft event loop. */
public interface RaftTransport {
    CompletableFuture<VoteResponse> requestVote(String peer, String group, VoteRequest request);
    CompletableFuture<AppendResponse> appendEntries(String peer, String group, AppendRequest request);
    default CompletableFuture<InstallSnapshotResponse> installSnapshot(String peer, String group,
                                                                       InstallSnapshotRequest request) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Snapshot RPC is not configured"));
    }
}
