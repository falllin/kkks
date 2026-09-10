package io.nexusstore.common;

public final class RpcException extends RuntimeException {
    private final String leaderId;

    public RpcException(String message, String leaderId) {
        super(message);
        this.leaderId = leaderId;
    }

    public String leaderId() {
        return leaderId;
    }
}
