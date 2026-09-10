package io.nexusstore.server;

import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import java.util.Map;
import java.util.concurrent.CompletionException;

@RestControllerAdvice
public class ApiErrors {
    @ExceptionHandler(FileController.FileTooLargeException.class)
    public ResponseEntity<?> tooLarge() { return ResponseEntity.status(413).body(Map.of("error", "File exceeds 1 MiB demo limit")); }
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<?> invalid(IllegalArgumentException e) { return ResponseEntity.badRequest().body(Map.of("error", e.getMessage())); }
    @ExceptionHandler({IllegalStateException.class, CompletionException.class})
    public ResponseEntity<?> unavailable(RuntimeException e) {
        Throwable cause = NodeRuntime.unwrap(e);
        if (cause instanceof IllegalArgumentException bad) return invalid(bad);
        return ResponseEntity.status(503).header("Retry-After", "1").body(Map.of("error", String.valueOf(cause.getMessage()),
                "writeOutcome", "unknown on timeout; retry the same operation with the same X-Request-Id"));
    }
}
