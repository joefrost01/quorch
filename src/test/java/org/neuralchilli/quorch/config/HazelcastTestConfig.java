package org.neuralchilli.quorch.config;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.quarkus.test.Mock;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test-specific Hazelcast configuration.
 * Creates an isolated, in-memory instance for testing.
 */
@Mock
@Alternative
@ApplicationScoped
public class HazelcastTestConfig {

    private static final Logger log = LoggerFactory.getLogger(HazelcastTestConfig.class);

    @ConfigProperty(name = "hazelcast.client.cluster-name", defaultValue = "orchestrator-test")
    String clusterName;

    private HazelcastInstance instance;

    @Produces
    @Singleton
    @Alternative
    public HazelcastInstance hazelcastInstance() {
        if (instance != null) {
            return instance;
        }

        log.info("Creating test Hazelcast instance with cluster: {}", clusterName);

        Config config = new Config();
        config.setClusterName(clusterName);

        // Disable all network features for isolated testing
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);

        // Disable phone home
        config.setProperty("hazelcast.phone.home.enabled", "false");

        // Fast operation timeout for tests
        config.setProperty("hazelcast.operation.call.timeout.millis", "5000");

        // Ensure writes are immediately visible (critical for tests)
        config.getMapConfig("*").setBackupCount(0).setAsyncBackupCount(0);

        // Disable metrics for faster tests
        config.getMetricsConfig().setEnabled(false);

        instance = Hazelcast.newHazelcastInstance(config);

        log.info("Test Hazelcast instance created successfully");

        return instance;
    }

    @PreDestroy
    void cleanup() {
        if (instance != null && instance.getLifecycleService().isRunning()) {
            log.info("Shutting down test Hazelcast instance");
            instance.getLifecycleService().shutdown();
            instance = null;
        }
    }
}