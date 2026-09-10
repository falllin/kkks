package io.nexusstore.common;

import com.fasterxml.jackson.databind.JsonNode;

public record RpcResponse(long id, boolean success, JsonNode body, String error, String leaderId) {
    public static RpcResponse ok(long id, Object body) {
        return new RpcResponse(id, true, Json.MAPPER.valueToTree(body), null, null);
    }

    public static RpcResponse fail(long id, String error, String leaderId) {
        return new RpcResponse(id, false, null, error, leaderId);
    }
}
