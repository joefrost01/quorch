package org.neuralchilli.quorch.config;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configures and produces the Hazelcast instance.
 * Uses Java serialization for domain objects.
 */
@ApplicationScoped
public class HazelcastConfig {

    private static final Logger log = LoggerFactory.getLogger(HazelcastConfig.class);

    @ConfigProperty(name = "hazelcast.client.cluster-name", defaultValue = "orchestrator-dev")
    String clusterName;

    @Produces
    @Singleton
    @Startup
    public HazelcastInstance hazelcastInstance() {
        log.info("Initializing Hazelcast with cluster name: {}", clusterName);

        Config config = new Config();
        config.setClusterName(clusterName);

        // Disable network join for embedded instance
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);

        // Configure serialization to prefer Java serialization
        SerializationConfig serializationConfig = config.getSerializationConfig();

        // Disable compression and shared objects
        serializationConfig.setEnableCompression(false);
        serializationConfig.setEnableSharedObject(false);

        // Allow class definition errors (more lenient)
        serializationConfig.setCheckClassDefErrors(false);

        log.info("Hazelcast configured to use Java serialization for domain objects");

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

        log.info("Hazelcast instance created successfully");

        return instance;
    }
}