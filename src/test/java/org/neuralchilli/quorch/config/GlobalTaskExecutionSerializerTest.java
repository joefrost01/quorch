package org.neuralchilli.quorch.config;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neuralchilli.quorch.domain.GlobalTaskExecution;
import org.neuralchilli.quorch.domain.TaskExecutionKey;
import org.neuralchilli.quorch.domain.TaskStatus;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for GlobalTaskExecutionSerializer.
 * Uses a shared, minimal Hazelcast instance for fast testing.
 */
class GlobalTaskExecutionSerializerTest {

    private static HazelcastInstance hazelcast;
    private static IMap<TaskExecutionKey, GlobalTaskExecution> testMap;

    @BeforeAll
    static void setupClass() {
        Config config = new Config();
        config.setClusterName("test-serializer-" + System.currentTimeMillis());
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);

        config.getSerializationConfig()
                .addSerializerConfig(new SerializerConfig()
                        .setTypeClass(GlobalTaskExecution.class)
                        .setImplementation(new GlobalTaskExecutionSerializer()));

        hazelcast = Hazelcast.newHazelcastInstance(config);
        testMap = hazelcast.getMap("test-global-tasks");
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
        TaskExecutionKey key = TaskExecutionKey.of("load-market-data", "load_market_2025-10-17_us");

        GlobalTaskExecution original = new GlobalTaskExecution(
                id,
                "load-market-data",
                "load_market_2025-10-17_us",
                Map.of("batch_date", "2025-10-17", "region", "us"),
                TaskStatus.PENDING,
                null, null, null, null,
                Map.of(),
                null
        );

        // When
        testMap.put(key, original);
        GlobalTaskExecution retrieved = testMap.get(key);

        // Then
        assertThat(retrieved).isNotNull();
        assertThat(retrieved.id()).isEqualTo(original.id());
        assertThat(retrieved.taskName()).isEqualTo(original.taskName());
        assertThat(retrieved.resolvedKey()).isEqualTo(original.resolvedKey());
        assertThat(retrieved.params()).isEqualTo(original.params());
        assertThat(retrieved.status()).isEqualTo(original.status());
    }

    @Test
    void shouldSerializeRunningGlobalTask() {
        // Given
        UUID id = UUID.randomUUID();
        TaskExecutionKey key = TaskExecutionKey.of("bootstrap-curves", "bootstrap_2025-10-17_us_v2");
        // Use truncated instant for millisecond precision
        Instant startedAt = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS);

        GlobalTaskExecution original = new GlobalTaskExecution(
                id,
                "bootstrap-curves",
                "bootstrap_2025-10-17_us_v2",
                Map.of("date", "2025-10-17", "region", "us", "model_version", "v2"),
                TaskStatus.RUNNING,
                "worker-3",
                "worker-3-thread-2",
                startedAt,
                null,
                Map.of(),
                null
        );

        // When
        testMap.put(key, original);
        GlobalTaskExecution retrieved = testMap.get(key);

        // Then
        assertThat(retrieved.status()).isEqualTo(TaskStatus.RUNNING);
        assertThat(retrieved.workerId()).isEqualTo("worker-3");
        assertThat(retrieved.threadName()).isEqualTo("worker-3-thread-2");
        assertThat(retrieved.startedAt()).isEqualTo(startedAt);
    }


    @Test
    void shouldSerializeCompletedGlobalTask() {
        // Given
        UUID id = UUID.randomUUID();
        TaskExecutionKey key = TaskExecutionKey.of("extract-data", "extract_2025-10-17_eu");
        // Use truncated instants for millisecond precision
        Instant startedAt = Instant.now().minusSeconds(300).truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
        Instant completedAt = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS);

        GlobalTaskExecution original = new GlobalTaskExecution(
                id,
                "extract-data",
                "extract_2025-10-17_eu",
                Map.of("batch_date", "2025-10-17", "region", "eu"),
                TaskStatus.COMPLETED,
                "worker-1",
                "worker-1-thread-1",
                startedAt,
                completedAt,
                Map.of("rows_extracted", 2500000, "files_created", 50),
                null
        );

        // When
        testMap.put(key, original);
        GlobalTaskExecution retrieved = testMap.get(key);

        // Then
        assertThat(retrieved.status()).isEqualTo(TaskStatus.COMPLETED);
        assertThat(retrieved.completedAt()).isEqualTo(completedAt);
        assertThat(retrieved.result()).containsEntry("rows_extracted", 2500000);
        assertThat(retrieved.result()).containsEntry("files_created", 50);
    }

    @Test
    void shouldSerializeFailedGlobalTask() {
        // Given
        UUID id = UUID.randomUUID();
        TaskExecutionKey key = TaskExecutionKey.of("problematic-task", "problematic_2025-10-17");

        GlobalTaskExecution original = new GlobalTaskExecution(
                id,
                "problematic-task",
                "problematic_2025-10-17",
                Map.of("date", "2025-10-17"),
                TaskStatus.FAILED,
                "worker-2",
                "worker-2-thread-3",
                Instant.now().minusSeconds(60),
                Instant.now(),
                Map.of(),
                "Database connection timeout after 60 seconds"
        );

        // When
        testMap.put(key, original);
        GlobalTaskExecution retrieved = testMap.get(key);

        // Then
        assertThat(retrieved.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(retrieved.error()).isEqualTo("Database connection timeout after 60 seconds");
    }

    @Test
    void shouldHandleAllTaskStatuses() {
        for (TaskStatus status : TaskStatus.values()) {
            UUID id = UUID.randomUUID();
            TaskExecutionKey key = TaskExecutionKey.of("status-test", "status_test_" + status.name());

            GlobalTaskExecution original = new GlobalTaskExecution(
                    id,
                    "status-test",
                    "status_test_" + status.name(),
                    Map.of("status", status.name()),
                    status,
                    status == TaskStatus.RUNNING ? "worker-1" : null,
                    status == TaskStatus.RUNNING ? "worker-1-thread-1" : null,
                    status != TaskStatus.PENDING ? Instant.now() : null,
                    status.isTerminal() ? Instant.now() : null,
                    Map.of(),
                    status == TaskStatus.FAILED ? "Test error" : null
            );

            testMap.put(key, original);
            GlobalTaskExecution retrieved = testMap.get(key);

            assertThat(retrieved.status())
                    .as("Status %s should be preserved", status)
                    .isEqualTo(status);
        }
    }

    @Test
    void shouldHandleComplexParameters() {
        UUID id = UUID.randomUUID();
        TaskExecutionKey key = TaskExecutionKey.of("complex-params", "test");

        GlobalTaskExecution original = new GlobalTaskExecution(
                id,
                "complex-params-task",
                "complex_params_test",
                Map.of(
                        "string_param", "hello world",
                        "int_param", 42,
                        "long_param", 999999999L,
                        "double_param", 3.14159,
                        "bool_param", true,
                        "nested_map", Map.of("inner_key", "inner_value")
                ),
                TaskStatus.PENDING,
                null, null, null, null,
                Map.of(),
                null
        );

        testMap.put(key, original);
        GlobalTaskExecution retrieved = testMap.get(key);

        assertThat(retrieved.params()).containsEntry("string_param", "hello world");
        assertThat(retrieved.params()).containsEntry("int_param", 42);
        assertThat(retrieved.params()).containsEntry("bool_param", true);
    }

    @Test
    void shouldHandleNullOptionalFields() {
        UUID id = UUID.randomUUID();
        TaskExecutionKey key = TaskExecutionKey.of("null-fields", "test");

        GlobalTaskExecution original = new GlobalTaskExecution(
                id,
                "null-fields-task",
                "null_fields_test",
                Map.of("param", "value"),
                TaskStatus.PENDING,
                null, null, null, null,
                Map.of(),
                null
        );

        testMap.put(key, original);
        GlobalTaskExecution retrieved = testMap.get(key);

        assertThat(retrieved.workerId()).isNull();
        assertThat(retrieved.threadName()).isNull();
        assertThat(retrieved.startedAt()).isNull();
        assertThat(retrieved.completedAt()).isNull();
        assertThat(retrieved.error()).isNull();
    }

    @Test
    void shouldHaveCorrectTypeId() {
        GlobalTaskExecutionSerializer serializer = new GlobalTaskExecutionSerializer();
        assertThat(serializer.getTypeId()).isEqualTo(1003);
    }
}