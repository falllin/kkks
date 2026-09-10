package io.nexusstore.raft;

public record CommandResult(boolean found, byte[] value, long index) {
    public CommandResult {
        value = value == null ? null : value.clone();
    }

    @Override
    public byte[] value() {
        return value == null ? null : value.clone();
    }
}
