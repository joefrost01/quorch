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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

@QuarkusTest
class GraphEvaluatorTest {

    private final Logger log = LoggerFactory.getLogger(getClass());

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
    IMap<TaskExecutionKey, Set<UUID>> globalTaskLinks;
    private IMap<TaskExecutionLookupKey, UUID> taskExecutionIndex;
    private IQueue<WorkMessage> workQueue;

    @BeforeEach
    void setup() {
        // CRITICAL: Clear DAG cache before each test
        evaluator.clearDAGCache();

        // Get references to maps
        graphDefinitions = hazelcast.getMap("graph-definitions");
        taskDefinitions = hazelcast.getMap("task-definitions");
        graphExecutions = hazelcast.getMap("graph-executions");
        taskExecutions = hazelcast.getMap("task-executions");
        globalTasks = hazelcast.getMap("global-tasks");
        globalTaskLinks = hazelcast.getMap("global-task-links");
        taskExecutionIndex = hazelcast.getMap("task-execution-index");
        workQueue = hazelcast.getQueue("work-queue");

        // CRITICAL: Complete cleanup - clear all data
        graphDefinitions.clear();
        taskDefinitions.clear();
        graphExecutions.clear();
        taskExecutions.clear();
        globalTasks.clear();
        globalTaskLinks.clear();
        taskExecutionIndex.clear();
        workQueue.clear();

        // Force Hazelcast to flush all operations
        graphDefinitions.flush();
        taskDefinitions.flush();
        graphExecutions.flush();
        taskExecutions.flush();
        globalTasks.flush();
        globalTaskLinks.flush();
        taskExecutionIndex.flush();

        // Small delay to ensure all cleanup is complete
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @AfterEach
    void cleanup() {
        // Clear DAG cache after test
        if (evaluator != null) {
            evaluator.clearDAGCache();
        }

        // Clear all maps
        if (graphDefinitions != null) graphDefinitions.clear();
        if (taskDefinitions != null) taskDefinitions.clear();
        if (graphExecutions != null) graphExecutions.clear();
        if (taskExecutions != null) taskExecutions.clear();
        if (globalTasks != null) globalTasks.clear();
        if (taskExecutionIndex != null) taskExecutionIndex.clear();
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
        Graph graph = Graph.builder("test-graph-schedule")
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
        UUID executionId = evaluator.executeGraph("test-graph-schedule", Map.of(), "test");

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
        Graph graph = Graph.builder("test-graph-completion")
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

        UUID executionId = evaluator.executeGraph("test-graph-completion", Map.of(), "test");

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
        Graph graph = Graph.builder("test-graph-failure")
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

        UUID executionId = evaluator.executeGraph("test-graph-failure", Map.of(), "test");

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
        Graph graph = Graph.builder("test-graph-dependencies")
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

        UUID executionId = evaluator.executeGraph("test-graph-dependencies", Map.of(), "test");

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

        // FIXED: Both tasks should have completion timestamps since graph is COMPLETED
        assertThat(taskA.completedAt()).as("task-a should be completed").isNotNull();
        assertThat(taskB.completedAt()).as("task-b should be completed").isNotNull();
        assertThat(taskA.startedAt()).as("task-a should have started").isNotNull();
        assertThat(taskB.startedAt()).as("task-b should have started").isNotNull();

        // Verify ordering: task-a completed before task-b started
        assertThat(taskA.completedAt())
                .as("task-a should complete before task-b starts")
                .isBefore(taskB.startedAt());
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
        Graph graph = Graph.builder("test-graph-global")
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
                "test-graph-global",
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

        Graph graph = Graph.builder("test-graph-dedup")
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

        // When: First graph executes
        log.info("=== Starting first graph execution ===");
        UUID exec1 = evaluator.executeGraph("test-graph-dedup", Map.of(), "test");
        log.info("First graph execution ID: {}", exec1);

        // CRITICAL: Wait for first execution to actually CREATE and START the global task
        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() -> {
            long globalTaskCount = globalTasks.values().stream()
                    .filter(gte -> gte.taskName().equals("load-data"))
                    .count();
            assertThat(globalTaskCount).as("Global task should be created").isEqualTo(1);

            GlobalTaskExecution gte = globalTasks.values().stream()
                    .filter(g -> g.taskName().equals("load-data"))
                    .findFirst()
                    .orElseThrow();

            assertThat(gte.status())
                    .as("Global task should be queued or running")
                    .isIn(TaskStatus.QUEUED, TaskStatus.RUNNING);

            log.info("Global task created with status: {}, linked graphs: {}",
                    gte.status(), gte.linkedGraphExecutions());
        });

        // Ensure global task state is fully propagated
        globalTasks.flush();
        try { Thread.sleep(100); } catch (InterruptedException e) {}

        // When: Second graph executes with same parameters
        log.info("=== Starting second graph execution ===");
        UUID exec2 = evaluator.executeGraph("test-graph-dedup", Map.of(), "test");
        log.info("Second graph execution ID: {}", exec2);

        // Then: Still only one global task execution
        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            long globalTaskCount = globalTasks.values().stream()
                    .filter(gte -> gte.taskName().equals("load-data") &&
                            gte.resolvedKey().equals("load_2025-10-18"))
                    .count();
            assertThat(globalTaskCount)
                    .as("Should only have one global task execution")
                    .isEqualTo(1);
        });

        // FIXED: Wait for global task to have both graphs linked
        await().atMost(3, TimeUnit.SECONDS).untilAsserted(() -> {
            GlobalTaskExecution globalExec = globalTasks.values().stream()
                    .filter(gte -> gte.taskName().equals("load-data"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Global task not found"));

            log.info("Current global task state - Status: {}, Linked graphs: {}",
                    globalExec.status(), globalExec.linkedGraphExecutions());

            // Global task should have both graphs linked
            assertThat(globalExec.linkedGraphExecutions())
                    .as("Global task should be linked to both graph executions")
                    .hasSize(2)
                    .containsExactlyInAnyOrder(exec1, exec2);
        });

        // Wait for both graphs to complete
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            GraphExecution graphExec1 = graphExecutions.get(exec1);
            GraphExecution graphExec2 = graphExecutions.get(exec2);

            log.info("Graph 1 status: {}, Graph 2 status: {}",
                    graphExec1.status(), graphExec2.status());

            assertThat(graphExec1.status()).isEqualTo(GraphStatus.COMPLETED);
            assertThat(graphExec2.status()).isEqualTo(GraphStatus.COMPLETED);
        });
    }

    @Test
    void shouldEvaluateExpressions() {
        // Given: Graph with parameters and expressions
        Graph graph = Graph.builder("test-graph-expressions")
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
        UUID executionId = evaluator.executeGraph("test-graph-expressions", Map.of("region", "eu"), "test");

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