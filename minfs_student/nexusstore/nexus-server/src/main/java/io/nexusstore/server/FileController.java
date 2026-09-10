package io.nexusstore.server;

import io.nexusstore.raft.Command;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import java.io.IOException;
import java.security.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api")
public class FileController {
    private final NodeRuntime runtime;
    private final PeerDirectory directory;
    public FileController(NodeRuntime runtime, PeerDirectory directory) { this.runtime = runtime; this.directory = directory; }

    @PutMapping("/files")
    public CompletableFuture<ResponseEntity<Map<String,Object>>> put(@RequestParam String key,
            @RequestHeader(value="X-Request-Id", required=false) String suppliedId, HttpServletRequest request, HttpServletResponse response) throws IOException {
        FileRules.validateKey(key);
        String id = requestId(suppliedId);
        response.setHeader("X-Request-Id", id);
        if (request.getContentLengthLong() > FileRules.MAX_FILE_BYTES) throw new FileTooLargeException();
        byte[] value = request.getInputStream().readNBytes(FileRules.MAX_FILE_BYTES + 1);
        if (value.length > FileRules.MAX_FILE_BYTES) throw new FileTooLargeException();
        return runtime.execute(new Command(Command.Type.PUT, key, value, id)).thenApply(result -> ResponseEntity.ok()
                .header("X-Request-Id", id).body(Map.of("key", key, "size", value.length, "shard", runtime.shardFor(key), "index", result.index(), "requestId", id)));
    }

    @GetMapping("/files")
    public CompletableFuture<ResponseEntity<byte[]>> get(@RequestParam String key) {
        return runtime.execute(new Command(Command.Type.GET, key, null, UUID.randomUUID().toString())).thenApply(result -> {
            if (!result.found()) return ResponseEntity.notFound().build();
            byte[] data = result.value();
            return ResponseEntity.ok().contentType(MediaType.APPLICATION_OCTET_STREAM).contentLength(data.length)
                    .header("X-Raft-Index", Long.toString(result.index())).cacheControl(CacheControl.noStore()).eTag(sha256(data)).body(data);
        });
    }

    @DeleteMapping("/files")
    public CompletableFuture<ResponseEntity<Map<String,Object>>> delete(@RequestParam String key,
            @RequestHeader(value="X-Request-Id", required=false) String suppliedId, HttpServletResponse response) {
        String id = requestId(suppliedId);
        response.setHeader("X-Request-Id", id);
        return runtime.execute(new Command(Command.Type.DELETE, key, null, id)).thenApply(result -> ResponseEntity.ok()
                .header("X-Request-Id", id).body(Map.of("key", key, "deleted", result.found(), "index", result.index(), "requestId", id)));
    }

    @GetMapping("/route")
    public Map<String,String> route(@RequestParam String key) { return Map.of("key", key, "shard", runtime.shardFor(key)); }

    @GetMapping("/cluster")
    public Map<String,Object> cluster() {
        return Map.of("nodeId", runtime.nodeId(), "shardCount", runtime.shardCount(), "shards", runtime.statuses(),
                "peers", directory.seeds(), "discoveredPeers", directory.discovered(), "maxFileBytes", FileRules.MAX_FILE_BYTES);
    }

    @PostMapping("/admin/compact")
    public CompletableFuture<Map<String,Object>> compact() {
        return runtime.compactAll().thenApply(states -> Map.of("nodeId", runtime.nodeId(), "shards", states));
    }

    private static String requestId(String supplied) { String id = supplied == null ? UUID.randomUUID().toString() : supplied; FileRules.validateRequestId(id); return id; }
    private static String sha256(byte[] value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value)); }
        catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
    public static class FileTooLargeException extends RuntimeException { }
}
