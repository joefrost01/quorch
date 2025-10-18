package org.neuralchilli.quorch.config;

import com.hazelcast.core.HazelcastInstance;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Test-specific Hazelcast configuration.
 * Uses the same HazelcastConfig as production but ensures cleanup.
 */
@ApplicationScoped
public class HazelcastTestProducer {

    private HazelcastInstance instance;

    @PreDestroy
    void cleanup() {
        if (instance != null && instance.getLifecycleService().isRunning()) {
            instance.getLifecycleService().shutdown();
        }
    }
}