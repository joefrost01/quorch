package org.neuralchilli.quorch.config;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neuralchilli.quorch.domain.GraphExecution;
import org.neuralchilli.quorch.domain.GraphStatus;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for GraphExecutionSerializer.
 * Uses a shared, minimal Hazelcast instance for fast testing.
 */
class GraphExecutionSerializerTest {

    private static HazelcastInstance hazelcast;
    private static IMap<UUID, GraphExecution> testMap;

    @BeforeAll
    static void setupClass() {
        Config config = new Config();
        config.setClusterName("test-graph-serializer-" + System.currentTimeMillis());
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);

        config.getSerializationConfig()
                .addSerializerConfig(new SerializerConfig()
                        .setTypeClass(GraphExecution.class)
                        .setImplementation(new GraphExecutionSerializer()));

        hazelcast = Hazelcast.newHazelcastInstance(config);
        testMap = hazelcast.getMap("test-graph-executions");
    }

    @AfterAll
    static void teardownClass() {
        if (hazelcast != null) {
            hazelcast.shutdown();
        }
    }

    @Test
    void shouldSerializeAndDeserializeBasicExecution() {
        // Given
        UUID id = UUID.randomUUID();
        Instant startedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        GraphExecution original = new GraphExecution(
                id,
                "test-graph",
                Map.of("date", "2025-10-17", "region", "us"),
                GraphStatus.RUNNING,
                "scheduler",
                startedAt,
                null,
                null
        );

        // When
        testMap.put(id, original);
        GraphExecution retrieved = testMap.get(id);

        // Then
        assertThat(retrieved).isNotNull();
        assertThat(retrieved.id()).isEqualTo(original.id());
        assertThat(retrieved.graphName()).isEqualTo(original.graphName());
        assertThat(retrieved.params()).isEqualTo(original.params());
        assertThat(retrieved.status()).isEqualTo(original.status());
        assertThat(retrieved.triggeredBy()).isEqualTo(original.triggeredBy());
        assertThat(retrieved.startedAt()).isEqualTo(original.startedAt());
        assertThat(retrieved.completedAt()).isNull();
        assertThat(retrieved.error()).isNull();
    }

    @Test
    void shouldSerializeCompletedExecution() {
        // Given
        UUID id = UUID.randomUUID();
        Instant startedAt = Instant.now().minusSeconds(300).truncatedTo(ChronoUnit.MILLIS);
        Instant completedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        GraphExecution original = new GraphExecution(
                id,
                "completed-graph",
                Map.of("batch_id", 12345, "full_refresh", true),
                GraphStatus.COMPLETED,
                "webhook",
                startedAt,
                completedAt,
                null
        );

        // When
        testMap.put(id, original);
        GraphExecution retrieved = testMap.get(id);

        // Then
        assertThat(retrieved.id()).isEqualTo(original.id());
        assertThat(retrieved.status()).isEqualTo(GraphStatus.COMPLETED);
        assertThat(retrieved.completedAt()).isEqualTo(completedAt);
    }

    @Test
    void shouldSerializeFailedExecution() {
        // Given
        UUID id = UUID.randomUUID();
        Instant startedAt = Instant.now().minusSeconds(100).truncatedTo(ChronoUnit.MILLIS);
        Instant completedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        GraphExecution original = new GraphExecution(
                id,
                "failed-graph",
                Map.of("attempt", 3),
                GraphStatus.FAILED,
                "manual",
                startedAt,
                completedAt,
                "Task 'critical-step' failed after 3 retries"
        );

        // When
        testMap.put(id, original);
        GraphExecution retrieved = testMap.get(id);

        // Then
        assertThat(retrieved.status()).isEqualTo(GraphStatus.FAILED);
        assertThat(retrieved.error()).isEqualTo(original.error());
    }

    @Test
    void shouldHandleEmptyParams() {
        // Given
        UUID id = UUID.randomUUID();
        Instant startedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        GraphExecution original = new GraphExecution(
                id,
                "no-params-graph",
                Map.of(),
                GraphStatus.RUNNING,
                "cron",
                startedAt,
                null,
                null
        );

        // When
        testMap.put(id, original);
        GraphExecution retrieved = testMap.get(id);

        // Then
        assertThat(retrieved.params()).isEmpty();
    }

    @Test
    void shouldHandleComplexParamTypes() {
        // Given
        UUID id = UUID.randomUUID();
        Instant startedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        GraphExecution original = new GraphExecution(
                id,
                "complex-params-graph",
                Map.of(
                        "string_param", "hello",
                        "int_param", 42,
                        "long_param", 999999999L,
                        "double_param", 3.14159,
                        "bool_param", true
                ),
                GraphStatus.RUNNING,
                "api",
                startedAt,
                null,
                null
        );

        // When
        testMap.put(id, original);
        GraphExecution retrieved = testMap.get(id);

        // Then
        assertThat(retrieved.params()).containsEntry("string_param", "hello");
        assertThat(retrieved.params()).containsEntry("int_param", 42);
        assertThat(retrieved.params()).containsEntry("long_param", 999999999L);
        assertThat(retrieved.params()).containsEntry("double_param", 3.14159);
        assertThat(retrieved.params()).containsEntry("bool_param", true);
    }

    @Test
    void shouldHandleAllStatuses() {
        for (GraphStatus status : GraphStatus.values()) {
            UUID id = UUID.randomUUID();
            Instant startedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);

            GraphExecution original = new GraphExecution(
                    id,
                    "status-test-" + status.name(),
                    Map.of("status", status.name()),
                    status,
                    "test",
                    startedAt,
                    status.isTerminal() ? Instant.now().truncatedTo(ChronoUnit.MILLIS) : null,
                    null
            );

            testMap.put(id, original);
            GraphExecution retrieved = testMap.get(id);

            assertThat(retrieved.status())
                    .as("Status %s should be preserved", status)
                    .isEqualTo(status);
        }
    }

    @Test
    void shouldHandleNullOptionalFields() {
        // Given
        UUID id = UUID.randomUUID();
        Instant startedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        GraphExecution original = new GraphExecution(
                id,
                "null-fields-graph",
                Map.of(),
                GraphStatus.RUNNING,
                "test",
                startedAt,
                null,
                null
        );

        // When
        testMap.put(id, original);
        GraphExecution retrieved = testMap.get(id);

        // Then
        assertThat(retrieved.completedAt()).isNull();
        assertThat(retrieved.error()).isNull();
    }

    @Test
    void shouldHaveCorrectTypeId() {
        GraphExecutionSerializer serializer = new GraphExecutionSerializer();
        assertThat(serializer.getTypeId()).isEqualTo(1001);
    }
}