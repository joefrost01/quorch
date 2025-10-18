package org.neuralchilli.quorch.config;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.quarkus.test.Mock;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.neuralchilli.quorch.domain.Parameter;
import org.neuralchilli.quorch.domain.TaskReference;

/**
 * Provides an embedded Hazelcast instance for tests.
 * This avoids the need for an external Hazelcast cluster.
 */
public class HazelcastTestProducer {

    private static HazelcastInstance instance;

    @Produces
    @ApplicationScoped
    @Mock
    public HazelcastInstance hazelcastInstance() {
        if (instance == null) {
            Config config = new Config();
            config.setClusterName("test-cluster");
            config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
            config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);

            // Register custom serializers
            SerializationConfig serializationConfig = config.getSerializationConfig();
            serializationConfig.addSerializerConfig(
                    new com.hazelcast.config.SerializerConfig()
                            .setTypeClass(Parameter.class)
                            .setImplementation(new HazelcastSerializers.ParameterSerializer())
            );
            serializationConfig.addSerializerConfig(
                    new com.hazelcast.config.SerializerConfig()
                            .setTypeClass(TaskReference.class)
                            .setImplementation(new HazelcastSerializers.TaskReferenceSerializer())
            );

            instance = Hazelcast.newHazelcastInstance(config);
        }
        return instance;
    }
}