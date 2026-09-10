package io.nexusstore.raft;

/** GET is deliberately a replicated command: every read needs a fresh majority. */
public record Command(Type type, String key, byte[] value, String requestId) {
    public enum Type { PUT, GET, DELETE, NOOP }

    public Command {
        value = value == null ? null : value.clone();
    }

    @Override
    public byte[] value() {
        return value == null ? null : value.clone();
    }
}
