package io.nexusstore.common;

import com.fasterxml.jackson.databind.JsonNode;

public record RpcRequest(long id, String method, String group, JsonNode body) { }
