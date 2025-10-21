package org.neuralchilli.quorch.config;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neuralchilli.quorch.domain.TaskExecution;
import org.neuralchilli.quorch.domain.TaskExecutionKey;
import org.neuralchilli.quorch.domain.TaskStatus;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for TaskExecutionSerializer.
 * Uses a shared, minimal Hazelcast instance for fast testing.
 */
class TaskExecutionSerializerTest {

    private static HazelcastInstance hazelcast;
    private static IMap<UUID, TaskExecution> testMap;

    @BeforeAll
    static void setupClass() {
        Config config = new Config();
        config.setClusterName("test-task-serializer-" + System.currentTimeMillis());
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);

        config.getSerializationConfig()
                .addSerializerConfig(new SerializerConfig()
                        .setTypeClass(TaskExecution.class)
                        .setImplementation(new TaskExecutionSerializer()));

        hazelcast = Hazelcast.newHazelcastInstance(config);
        testMap = hazelcast.getMap("test-task-executions");
    }

    @AfterAll
    static void teardownClass() {
        if (hazelcast != null) {
            hazelcast.shutdown();
        }
    }

    @Test
    void shouldSerializeBasicTaskExecution() {
        // Given
        UUID id = UUID.randomUUID();
        UUID graphExecId = UUID.randomUUID();

        TaskExecution original = new TaskExecution(
                id,
                graphExecId,
                "extract-data",
                TaskStatus.PENDING,
                null, null, null, null, null,
                Map.of(),
                null,
                1,
                false,
                null
        );

        // When
        testMap.put(id, original);
        TaskExecution retrieved = testMap.get(id);

        // Then
        assertThat(retrieved).isNotNull();
        assertThat(retrieved.id()).isEqualTo(original.id());
        assertThat(retrieved.graphExecutionId()).isEqualTo(original.graphExecutionId());
        assertThat(retrieved.taskName()).isEqualTo(original.taskName());
        assertThat(retrieved.status()).isEqualTo(original.status());
        assertThat(retrieved.attempt()).isEqualTo(1);
        assertThat(retrieved.isGlobal()).isFalse();
    }

    @Test
    void shouldSerializeRunningTaskExecution() {
        // Given
        UUID id = UUID.randomUUID();
        UUID graphExecId = UUID.randomUUID();
        Instant startedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        TaskExecution original = new TaskExecution(
                id,
                graphExecId,
                "transform-data",
                TaskStatus.RUNNING,
                "worker-1",
                "worker-1-thread-3",
                startedAt,
                null, null,
                Map.of(),
                null,
                1,
                false,
                null
        );

        // When
        testMap.put(id, original);
        TaskExecution retrieved = testMap.get(id);

        // Then
        assertThat(retrieved.status()).isEqualTo(TaskStatus.RUNNING);
        assertThat(retrieved.workerId()).isEqualTo("worker-1");
        assertThat(retrieved.threadName()).isEqualTo("worker-1-thread-3");
        assertThat(retrieved.startedAt()).isEqualTo(startedAt);
    }

    @Test
    void shouldSerializeCompletedTaskExecution() {
        // Given
        UUID id = UUID.randomUUID();
        UUID graphExecId = UUID.randomUUID();
        Instant startedAt = Instant.now().minusSeconds(120).truncatedTo(ChronoUnit.MILLIS);
        Instant completedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        TaskExecution original = new TaskExecution(
                id,
                graphExecId,
                "load-data",
                TaskStatus.COMPLETED,
                "worker-2",
                "worker-2-thread-1",
                startedAt,
                completedAt,
                120_000L,
                Map.of("rows_loaded", 150000, "duration_seconds", 120),
                null,
                1,
                false,
                null
        );

        // When
        testMap.put(id, original);
        TaskExecution retrieved = testMap.get(id);

        // Then
        assertThat(retrieved.status()).isEqualTo(TaskStatus.COMPLETED);
        assertThat(retrieved.completedAt()).isEqualTo(completedAt);
        assertThat(retrieved.durationMillis()).isEqualTo(120_000L);
        assertThat(retrieved.result()).containsEntry("rows_loaded", 150000);
    }

    @Test
    void shouldSerializeFailedTaskExecution() {
        // Given
        UUID id = UUID.randomUUID();
        UUID graphExecId = UUID.randomUUID();
        Instant startedAt = Instant.now().minusSeconds(30).truncatedTo(ChronoUnit.MILLIS);
        Instant completedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        TaskExecution original = new TaskExecution(
                id,
                graphExecId,
                "flaky-task",
                TaskStatus.FAILED,
                "worker-3",
                "worker-3-thread-2",
                startedAt,
                completedAt,
                30_000L,
                Map.of(),
                "Connection timeout after 30s",
                3,
                false,
                null
        );

        // When
        testMap.put(id, original);
        TaskExecution retrieved = testMap.get(id);

        // Then
        assertThat(retrieved.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(retrieved.error()).isEqualTo("Connection timeout after 30s");
        assertThat(retrieved.attempt()).isEqualTo(3);
    }

    @Test
    void shouldSerializeGlobalTaskExecution() {
        // Given
        UUID id = UUID.randomUUID();
        UUID graphExecId = UUID.randomUUID();
        TaskExecutionKey globalKey = TaskExecutionKey.of(
                "load-market-data",
                "load_market_2025-10-17_us"
        );
        Instant startedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        TaskExecution original = new TaskExecution(
                id,
                graphExecId,
                "load-market-data",
                TaskStatus.RUNNING,
                "worker-1",
                "worker-1-thread-1",
                startedAt,
                null, null,
                Map.of(),
                null,
                1,
                true,
                globalKey
        );

        // When
        testMap.put(id, original);
        TaskExecution retrieved = testMap.get(id);

        // Then
        assertThat(retrieved.isGlobal()).isTrue();
        assertThat(retrieved.globalKey()).isNotNull();
        assertThat(retrieved.globalKey().taskName()).isEqualTo("load-market-data");
        assertThat(retrieved.globalKey().resolvedKey()).isEqualTo("load_market_2025-10-17_us");
    }

    @Test
    void shouldHandleAllTaskStatuses() {
        for (TaskStatus status : TaskStatus.values()) {
            UUID id = UUID.randomUUID();
            UUID graphExecId = UUID.randomUUID();

            TaskExecution original = new TaskExecution(
                    id,
                    graphExecId,
                    "status-test-" + status.name(),
                    status,
                    status == TaskStatus.RUNNING ? "worker-1" : null,
                    status == TaskStatus.RUNNING ? "worker-1-thread-1" : null,
                    status != TaskStatus.PENDING ? Instant.now().truncatedTo(ChronoUnit.MILLIS) : null,
                    status.isTerminal() ? Instant.now().truncatedTo(ChronoUnit.MILLIS) : null,
                    status.isTerminal() ? 1000L : null,
                    Map.of(),
                    status == TaskStatus.FAILED ? "Test error" : null,
                    1,
                    false,
                    null
            );

            testMap.put(id, original);
            TaskExecution retrieved = testMap.get(id);

            assertThat(retrieved.status())
                    .as("Status %s should be preserved", status)
                    .isEqualTo(status);
        }
    }

    @Test
    void shouldHandleComplexResultTypes() {
        // Given
        UUID id = UUID.randomUUID();
        UUID graphExecId = UUID.randomUUID();
        Instant startedAt = Instant.now().minusSeconds(60).truncatedTo(ChronoUnit.MILLIS);
        Instant completedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        TaskExecution original = new TaskExecution(
                id,
                graphExecId,
                "complex-result-task",
                TaskStatus.COMPLETED,
                "worker-1",
                "worker-1-thread-1",
                startedAt,
                completedAt,
                60_000L,
                Map.of(
                        "string_value", "success",
                        "int_value", 42,
                        "long_value", 999999999L,
                        "double_value", 3.14159,
                        "bool_value", true
                ),
                null,
                1,
                false,
                null
        );

        // When
        testMap.put(id, original);
        TaskExecution retrieved = testMap.get(id);

        // Then
        assertThat(retrieved.result()).containsEntry("string_value", "success");
        assertThat(retrieved.result()).containsEntry("int_value", 42);
        assertThat(retrieved.result()).containsEntry("long_value", 999999999L);
        assertThat(retrieved.result()).containsEntry("bool_value", true);
    }

    @Test
    void shouldHandleNullOptionalFields() {
        // Given
        UUID id = UUID.randomUUID();
        UUID graphExecId = UUID.randomUUID();

        TaskExecution original = new TaskExecution(
                id,
                graphExecId,
                "minimal-task",
                TaskStatus.PENDING,
                null, null, null, null, null,
                Map.of(),
                null,
                1,
                false,
                null
        );

        // When
        testMap.put(id, original);
        TaskExecution retrieved = testMap.get(id);

        // Then
        assertThat(retrieved.workerId()).isNull();
        assertThat(retrieved.threadName()).isNull();
        assertThat(retrieved.startedAt()).isNull();
        assertThat(retrieved.completedAt()).isNull();
        assertThat(retrieved.durationMillis()).isNull();
        assertThat(retrieved.error()).isNull();
        assertThat(retrieved.globalKey()).isNull();
    }

    @Test
    void shouldHaveCorrectTypeId() {
        TaskExecutionSerializer serializer = new TaskExecutionSerializer();
        assertThat(serializer.getTypeId()).isEqualTo(1002);
    }
}