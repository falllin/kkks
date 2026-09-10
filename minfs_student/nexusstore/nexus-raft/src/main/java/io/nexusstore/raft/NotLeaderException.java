package io.nexusstore.raft;

public final class NotLeaderException extends RuntimeException {
    private final String leaderId;

    public NotLeaderException(String leaderId) {
        super(leaderId == null ? "No elected leader is known" : "Request must go to leader " + leaderId);
        this.leaderId = leaderId;
    }

    public String leaderId() { return leaderId; }
}
