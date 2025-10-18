package org.neuralchilli.quorch.config;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.neuralchilli.quorch.domain.Parameter;
import org.neuralchilli.quorch.domain.TaskReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configures and produces the Hazelcast instance with custom serializers.
 * Ensures consistent serialization across dev and prod environments.
 */
@ApplicationScoped
public class HazelcastConfig {

    private static final Logger log = LoggerFactory.getLogger(HazelcastConfig.class);

    @ConfigProperty(name = "hazelcast.client.cluster-name", defaultValue = "orchestrator-dev")
    String clusterName;

    @Produces
    @ApplicationScoped
    @Startup
    public HazelcastInstance hazelcastInstance() {
        log.info("Initializing Hazelcast with cluster name: {}", clusterName);

        Config config = new Config();
        config.setClusterName(clusterName);

        // Disable network join for embedded instance
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);

        // Configure serialization BEFORE registering serializers
        SerializationConfig serializationConfig = config.getSerializationConfig();

        // Disable compact serialization auto-detection for our types
        serializationConfig.setAllowUnsafe(true);
        serializationConfig.setEnableCompression(false);
        serializationConfig.setEnableSharedObject(false);

        // Register custom serializers with explicit configuration
        SerializerConfig paramConfig = new SerializerConfig();
        paramConfig.setTypeClass(Parameter.class);
        paramConfig.setImplementation(new HazelcastSerializers.ParameterSerializer());
        serializationConfig.addSerializerConfig(paramConfig);

        SerializerConfig taskRefConfig = new SerializerConfig();
        taskRefConfig.setTypeClass(TaskReference.class);
        taskRefConfig.setImplementation(new HazelcastSerializers.TaskReferenceSerializer());
        serializationConfig.addSerializerConfig(taskRefConfig);

        log.info("Registered custom serializers for Parameter and TaskReference");

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

        log.info("Hazelcast instance created successfully");

        return instance;
    }
}