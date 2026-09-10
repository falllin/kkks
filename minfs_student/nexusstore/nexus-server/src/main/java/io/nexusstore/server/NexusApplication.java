package io.nexusstore.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(NexusProperties.class)
public class NexusApplication {
    public static void main(String[] args) { SpringApplication.run(NexusApplication.class, args); }
}
