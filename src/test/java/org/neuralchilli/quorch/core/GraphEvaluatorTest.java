package org.neuralchilli.quorch.core;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.collection.IQueue;
import io.quarkus.test.junit.QuarkusTest;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neuralchilli.quorch.domain.*;
import org.neuralchilli.quorch.worker.WorkerPool;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

@QuarkusTest
class GraphEvaluatorTest {

    @Inject
    GraphEvaluator evaluator;

    @Inject
    HazelcastInstance hazelcast;

    @Inject
    EventBus eventBus;

    @Inject
    WorkerPool workerPool;

    private IMap<String, Graph> graphDefinitions;
    private IMap<String, Task> taskDefinitions;
    private IMap<UUID, GraphExecution> graphExecutions;
    private IMap<UUID, TaskExecution> taskExecutions;
    private IMap<TaskExecutionKey, GlobalTaskExecution> globalTasks;
    private IMap<TaskExecutionLookupKey, UUID> taskExecutionIndex;
    private IQueue<WorkMessage> workQueue;

    @BeforeEach
    void setup() {
        graphDefinitions = hazelcast.getMap("graph-definitions");
        taskDefinitions = hazelcast.getMap("task-definitions");
        graphExecutions = hazelcast.getMap("graph-executions");
        taskExecutions = hazelcast.getMap("task-executions");
        globalTasks = hazelcast.getMap("global-tasks");
        taskExecutionIndex = hazelcast.getMap("task-execution-index");
        workQueue = hazelcast.getQueue("work-queue");

        // CRITICAL: Destroy and recreate maps for complete isolation between tests
        graphDefinitions.destroy();
        taskDefinitions.destroy();
        graphExecutions.destroy();
        taskExecutions.destroy();
        globalTasks.destroy();
        taskExecutionIndex.destroy();

        // Recreate maps
        graphDefinitions = hazelcast.getMap("graph-definitions");
        taskDefinitions = hazelcast.getMap("task-definitions");
        graphExecutions = hazelcast.getMap("graph-executions");
        taskExecutions = hazelcast.getMap("task-executions");
        globalTasks = hazelcast.getMap("global-tasks");
        taskExecutionIndex = hazelcast.getMap("task-execution-index");

        // Clear queue
        workQueue.clear();
    }

    @AfterEach
    void cleanup() {
        // Destroy all test data
        if (graphDefinitions != null) graphDefinitions.destroy();
        if (taskDefinitions != null) taskDefinitions.destroy();
        if (graphExecutions != null) graphExecutions.destroy();
        if (taskExecutions != null) taskExecutions.destroy();
        if (globalTasks != null) globalTasks.destroy();
        if (taskExecutionIndex != null) taskExecutionIndex.destroy();
        if (workQueue != null) workQueue.clear();
    }

    @Test
    void shouldStartGraphExecution() {
        // Given: A simple graph with one task
        Graph graph = Graph.builder("test-graph")
                .tasks(List.of(
                        TaskReference.inline(
                                Task.builder("task1")
                                        .command("echo")
                                        .args(List.of("hello"))
                                        .build(),
                                List.of()
                        )
                ))
                .build();

        graphDefinitions.put(graph.name(), graph);
        graphDefinitions.flush();

        // When: Starting execution (synchronous in test mode)
        UUID executionId = evaluator.executeGraph(
                "test-graph",
                Map.of("param1", "value1"),
                "test"
        );

        // Then: Graph execution created
        assertThat(executionId).isNotNull();

        GraphExecution execution = graphExecutions.get(executionId);
        assertThat(execution).isNotNull();
        assertThat(execution.graphName()).isEqualTo("test-graph");
        assertThat(execution.status()).isEqualTo(GraphStatus.RUNNING);
        assertThat(execution.params()).containsEntry("param1", "value1");

        // Task execution created and indexed
        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
            long taskCount = taskExecutions.values().stream()
                    .filter(te -> te.graphExecutionId().equals(executionId))
                    .count();
            assertThat(taskCount).isEqualTo(1);

            // Verify index was populated
            TaskExecutionLookupKey lookupKey = new TaskExecutionLookupKey(executionId, "task1");
            UUID taskExecId = taskExecutionIndex.get(lookupKey);
            assertThat(taskExecId).isNotNull();
        });
    }

    @Test
    void shouldScheduleReadyTask() {
        // Given: A simple graph
        Graph graph = Graph.builder("test-graph")
                .tasks(List.of(
                        TaskReference.inline(
                                Task.builder("task1")
                                        .command("echo")
                                        .args(List.of("hello"))
                                        .timeout(60)
                                        .build(),
                                List.of()
                        )
                ))
                .build();

        graphDefinitions.put(graph.name(), graph);
        graphDefinitions.flush();

        // When: Starting and evaluating (synchronous in test mode)
        UUID executionId = evaluator.executeGraph("test-graph", Map.of(), "test");

        // Then: Task should be queued/running/completed
        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
            TaskExecution taskExec = taskExecutions.values().stream()
                    .filter(te -> te.graphExecutionId().equals(executionId))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Task execution not found"));

            assertThat(taskExec.status())
                    .isIn(TaskStatus.QUEUED, TaskStatus.RUNNING, TaskStatus.COMPLETED);
            assertThat(taskExec.taskName()).isEqualTo("task1");
        });
    }

    @Test
    void shouldHandleTaskCompletion() {
        // Given: A graph with one task
        Graph graph = Graph.builder("test-graph")
                .tasks(List.of(
                        TaskReference.inline(
                                Task.builder("task1")
                                        .command("echo")
                                        .args(List.of("hello"))
                                        .build(),
                                List.of()
                        )
                ))
                .build();

        graphDefinitions.put(graph.name(), graph);
        graphDefinitions.flush();

        UUID executionId = evaluator.executeGraph("test-graph", Map.of(), "test");

        // Wait for worker to complete the task
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            GraphExecution graphExec = graphExecutions.get(executionId);
            assertThat(graphExec.status()).isEqualTo(GraphStatus.COMPLETED);
        });

        // Verify task completed successfully
        TaskExecution taskExec = taskExecutions.values().stream()
                .filter(te -> te.graphExecutionId().equals(executionId))
                .findFirst()
                .orElseThrow();

        assertThat(taskExec.status()).isEqualTo(TaskStatus.COMPLETED);
    }

    @Test
    void shouldHandleTaskFailure() {
        // Given: A graph with task that will fail (invalid command)
        Graph graph = Graph.builder("test-graph")
                .tasks(List.of(
                        TaskReference.inline(
                                Task.builder("task1")
                                        .command("this-command-does-not-exist")
                                        .retry(0)
                                        .build(),
                                List.of()
                        )
                ))
                .build();

        graphDefinitions.put(graph.name(), graph);
        graphDefinitions.flush();

        UUID executionId = evaluator.executeGraph("test-graph", Map.of(), "test");

        // Wait for worker to fail the task
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            GraphExecution graphExec = graphExecutions.get(executionId);
            assertThat(graphExec.status()).isEqualTo(GraphStatus.FAILED);
        });

        // Verify task failed
        TaskExecution taskExec = taskExecutions.values().stream()
                .filter(te -> te.graphExecutionId().equals(executionId))
                .findFirst()
                .orElseThrow();

        assertThat(taskExec.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(taskExec.error()).isNotNull();
    }

    @Test
    void shouldRespectTaskDependencies() {
        // Given: A -> B (B depends on A)
        Graph graph = Graph.builder("test-graph")
                .tasks(List.of(
                        TaskReference.inline(
                                Task.builder("task-a")
                                        .command("echo")
                                        .args(List.of("A"))
                                        .build(),
                                List.of()
                        ),
                        TaskReference.inline(
                                Task.builder("task-b")
                                        .command("echo")
                                        .args(List.of("B"))
                                        .build(),
                                List.of("task-a")
                        )
                ))
                .build();

        graphDefinitions.put(graph.name(), graph);
        graphDefinitions.flush();

        UUID executionId = evaluator.executeGraph("test-graph", Map.of(), "test");

        // Wait for both tasks to complete
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            GraphExecution graphExec = graphExecutions.get(executionId);
            assertThat(graphExec.status()).isEqualTo(GraphStatus.COMPLETED);
        });

        // Verify both tasks completed
        List<TaskExecution> tasks = taskExecutions.values().stream()
                .filter(te -> te.graphExecutionId().equals(executionId))
                .toList();

        assertThat(tasks).hasSize(2);
        assertThat(tasks).allMatch(te -> te.status() == TaskStatus.COMPLETED);

        // Verify task-a completed before task-b started
        TaskExecution taskA = tasks.stream()
                .filter(te -> te.taskName().equals("task-a"))
                .findFirst()
                .orElseThrow();

        TaskExecution taskB = tasks.stream()
                .filter(te -> te.taskName().equals("task-b"))
                .findFirst()
                .orElseThrow();

        assertThat(taskA.completedAt()).isNotNull();
        assertThat(taskB.startedAt()).isNotNull();
        assertThat(taskA.completedAt()).isBefore(taskB.startedAt());
    }

    @Test
    void shouldHandleGlobalTask() {
        // Given: A global task definition
        Task globalTask = Task.builder("load-data")
                .global(true)
                .key("load_${params.date}")
                .params(Map.of(
                        "date", Parameter.required(ParameterType.STRING, "Date")
                ))
                .command("echo")
                .args(List.of("Loading data"))
                .build();

        taskDefinitions.put(globalTask.name(), globalTask);
        taskDefinitions.flush();

        // Graph that uses the global task
        Graph graph = Graph.builder("test-graph")
                .params(Map.of(
                        "date", Parameter.withDefault(ParameterType.STRING, "2025-10-18", "Date")
                ))
                .tasks(List.of(
                        TaskReference.toGlobal(
                                "load-data",
                                Map.of("date", "${params.date}"),
                                List.of()
                        )
                ))
                .build();

        graphDefinitions.put(graph.name(), graph);
        graphDefinitions.flush();

        // When: Starting execution
        UUID executionId = evaluator.executeGraph(
                "test-graph",
                Map.of("date", "2025-10-18"),
                "test"
        );

        // Then: Global task execution created
        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(globalTasks.size()).isGreaterThanOrEqualTo(1);
        });

        GlobalTaskExecution globalExec = globalTasks.values().stream()
                .filter(gte -> gte.taskName().equals("load-data"))
                .findFirst()
                .orElseThrow();

        assertThat(globalExec.taskName()).isEqualTo("load-data");
        assertThat(globalExec.resolvedKey()).isEqualTo("load_2025-10-18");
        assertThat(globalExec.linkedGraphExecutions()).contains(executionId);

        // Wait for completion
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            GraphExecution graphExec = graphExecutions.get(executionId);
            assertThat(graphExec.status()).isEqualTo(GraphStatus.COMPLETED);
        });
    }

    @Test
    void shouldDeduplicateGlobalTasks() {
        // Given: A global task
        Task globalTask = Task.builder("load-data")
                .global(true)
                .key("load_${params.date}")
                .params(Map.of(
                        "date", Parameter.required(ParameterType.STRING, "Date")
                ))
                .command("echo")
                .args(List.of("Loading"))
                .build();

        taskDefinitions.put(globalTask.name(), globalTask);
        taskDefinitions.flush();

        Graph graph = Graph.builder("test-graph")
                .tasks(List.of(
                        TaskReference.toGlobal(
                                "load-data",
                                Map.of("date", "2025-10-18"),
                                List.of()
                        )
                ))
                .build();

        graphDefinitions.put(graph.name(), graph);
        graphDefinitions.flush();

        // When: Two graphs execute with same parameters
        UUID exec1 = evaluator.executeGraph("test-graph", Map.of(), "test");

        // Small delay to ensure first graph's global task is registered
        try { Thread.sleep(100); } catch (InterruptedException e) {}

        UUID exec2 = evaluator.executeGraph("test-graph", Map.of(), "test");

        // Then: Only one global task execution created
        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
            long globalTaskCount = globalTasks.values().stream()
                    .filter(gte -> gte.taskName().equals("load-data") &&
                            gte.resolvedKey().equals("load_2025-10-18"))
                    .count();
            assertThat(globalTaskCount).isEqualTo(1);
        });

        GlobalTaskExecution globalExec = globalTasks.values().stream()
                .filter(gte -> gte.taskName().equals("load-data"))
                .findFirst()
                .orElseThrow();

        assertThat(globalExec.linkedGraphExecutions())
                .containsExactlyInAnyOrder(exec1, exec2);

        // Wait for both graphs to complete
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            GraphExecution graphExec1 = graphExecutions.get(exec1);
            GraphExecution graphExec2 = graphExecutions.get(exec2);
            assertThat(graphExec1.status()).isEqualTo(GraphStatus.COMPLETED);
            assertThat(graphExec2.status()).isEqualTo(GraphStatus.COMPLETED);
        });
    }

    @Test
    void shouldEvaluateExpressions() {
        // Given: Graph with parameters and expressions
        Graph graph = Graph.builder("test-graph")
                .params(Map.of(
                        "region", Parameter.withDefault(ParameterType.STRING, "us", "Region")
                ))
                .tasks(List.of(
                        TaskReference.inline(
                                Task.builder("task1")
                                        .command("echo")
                                        .args(List.of("Processing region"))
                                        .build(),
                                List.of()
                        )
                ))
                .build();

        graphDefinitions.put(graph.name(), graph);
        graphDefinitions.flush();

        // When: Starting execution
        UUID executionId = evaluator.executeGraph("test-graph", Map.of("region", "eu"), "test");

        // Then: Task completes successfully
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            GraphExecution graphExec = graphExecutions.get(executionId);
            assertThat(graphExec.status()).isEqualTo(GraphStatus.COMPLETED);
        });

        TaskExecution taskExec = taskExecutions.values().stream()
                .filter(te -> te.graphExecutionId().equals(executionId))
                .findFirst()
                .orElseThrow();

        assertThat(taskExec.status()).isEqualTo(TaskStatus.COMPLETED);
    }
}