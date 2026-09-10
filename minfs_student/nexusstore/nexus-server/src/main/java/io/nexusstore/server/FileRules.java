package io.nexusstore.server;

import java.nio.charset.StandardCharsets;

/** Logical keys never become local filesystem paths. */
public final class FileRules {
    public static final int MAX_FILE_BYTES = 1024 * 1024;
    private FileRules() { }
    public static void validateKey(String key) {
        if (key == null || key.isBlank() || key.getBytes(StandardCharsets.UTF_8).length > 512 || key.chars().anyMatch(Character::isISOControl))
            throw new IllegalArgumentException("key must be nonblank, no control characters, at most 512 UTF-8 bytes");
    }
    public static void validateRequestId(String id) {
        if (id == null || !id.matches("[a-zA-Z0-9._:-]{1,128}"))
            throw new IllegalArgumentException("X-Request-Id must contain 1..128 letters, digits, dot, colon, dash or underscore");
    }
}
