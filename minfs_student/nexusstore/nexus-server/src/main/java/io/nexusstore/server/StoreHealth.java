package io.nexusstore.server;

import org.springframework.boot.actuate.health.*;
import org.springframework.stereotype.Component;

/** Local health only. A quorum is established by each actual file operation. */
@Component("store")
public class StoreHealth implements HealthIndicator {
    private final NodeRuntime runtime;
    public StoreHealth(NodeRuntime runtime) { this.runtime = runtime; }
    @Override public Health health() {
        boolean healthy = runtime.isRunning() && runtime.statuses().stream()
                .noneMatch(s -> s.role().equals("FAILED") || s.role().equals("CLOSED"));
        return (healthy ? Health.up() : Health.down())
                .withDetail("nodeId", runtime.nodeId()).withDetail("shards", runtime.statuses())
                .withDetail("scope", "local process; use a file request to verify quorum").build();
    }
}
