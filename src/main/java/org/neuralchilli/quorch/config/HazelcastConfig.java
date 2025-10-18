package org.neuralchilli.quorch.config;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
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
    @Singleton
    @Startup
    public HazelcastInstance hazelcastInstance() {
        log.info("Initializing Hazelcast with cluster name: {}", clusterName);

        Config config = new Config();
        config.setClusterName(clusterName);

        // Disable network join for embedded instance
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);

        // Configure serialization
        SerializationConfig serializationConfig = config.getSerializationConfig();

        // Disable all auto-serialization mechanisms
        serializationConfig.setEnableCompression(false);
        serializationConfig.setEnableSharedObject(false);

        // Set global serializers to use StreamSerializer for everything not explicitly configured
        serializationConfig.setCheckClassDefErrors(false);

        // Register custom serializers with HIGH priority (lower number = higher priority)
        SerializerConfig paramConfig = new SerializerConfig();
        paramConfig.setTypeClass(Parameter.class);
        paramConfig.setClass(HazelcastSerializers.ParameterSerializer.class);
        serializationConfig.addSerializerConfig(paramConfig);

        SerializerConfig taskRefConfig = new SerializerConfig();
        taskRefConfig.setTypeClass(TaskReference.class);
        taskRefConfig.setClass(HazelcastSerializers.TaskReferenceSerializer.class);
        serializationConfig.addSerializerConfig(taskRefConfig);

        log.info("Registered custom serializers for Parameter and TaskReference");

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

        log.info("Hazelcast instance created successfully");

        return instance;
    }
}