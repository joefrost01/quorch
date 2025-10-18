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

    private IMap<String, Graph> graphDefinitions;
    private IMap<String, Task> taskDefinitions;
    private IMap<UUID, GraphExecution> graphExecutions;
    private IMap<UUID, TaskExecution> taskExecutions;
    private IMap<TaskExecutionKey, GlobalTaskExecution> globalTasks;
    private IQueue<WorkMessage> workQueue;

    // CRITICAL: Async operations need longer timeouts
    private static final int ASYNC_TIMEOUT_SECONDS = 10;

    @BeforeEach
    void setup() {
        graphDefinitions = hazelcast.getMap("graph-definitions");
        taskDefinitions = hazelcast.getMap("task-definitions");
        graphExecutions = hazelcast.getMap("graph-executions");
        taskExecutions = hazelcast.getMap("task-executions");
        globalTasks = hazelcast.getMap("global-tasks");
        workQueue = hazelcast.getQueue("work-queue");

        // Clear all maps
        graphDefinitions.clear();
        taskDefinitions.clear();
        graphExecutions.clear();
        taskExecutions.clear();
        globalTasks.clear();
        workQueue.clear();
    }

    @AfterEach
    void cleanup() {
        graphDefinitions.clear();
        taskDefinitions.clear();
        graphExecutions.clear();
        taskExecutions.clear();
        globalTasks.clear();
        workQueue.clear();
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

        // When: Starting execution
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

        // Task execution created (async, needs longer wait)
        await().atMost(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS).untilAsserted(() -> {
            long taskCount = taskExecutions.values().stream()
                    .filter(te -> te.graphExecutionId().equals(executionId))
                    .count();
            assertThat(taskCount).isEqualTo(1);
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

        // When: Starting and evaluating (async)
        UUID executionId = evaluator.executeGraph("test-graph", Map.of(), "test");

        // Then: Task should be queued to work queue (async, needs longer wait)
        await().atMost(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    assertThat(workQueue.size()).isGreaterThan(0);
                });

        WorkMessage work = workQueue.poll();
        assertThat(work).isNotNull();
        assertThat(work.taskName()).isEqualTo("task1");
        assertThat(work.command()).isEqualTo("echo");
        assertThat(work.args()).containsExactly("hello");
    }

    @Test
    void shouldHandleTaskCompletion() throws InterruptedException {
        // Given: A graph with one task
        Graph graph = Graph.builder("test-graph")
                .tasks(List.of(
                        TaskReference.inline(
                                Task.builder("task1")
                                        .command("echo")
                                        .build(),
                                List.of()
                        )
                ))
                .build();

        graphDefinitions.put(graph.name(), graph);

        UUID executionId = evaluator.executeGraph("test-graph", Map.of(), "test");

        // Wait for task to be created and queued (async)
        await().atMost(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    assertThat(workQueue.size()).isGreaterThan(0);
                });

        // Get task execution
        TaskExecution taskExec = taskExecutions.values().stream()
                .filter(te -> te.graphExecutionId().equals(executionId))
                .findFirst()
                .orElseThrow();

        // When: Task completes (fire event and wait for async processing)
        TaskCompletionEvent event = TaskCompletionEvent.of(
                taskExec.id(),
                Map.of("result", "success")
        );
        eventBus.send("task.completed", event);

        // Give async processing time to propagate
        Thread.sleep(1000);

        // Then: Task status updated and graph completes (async)
        await().atMost(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    TaskExecution updated = taskExecutions.get(taskExec.id());
                    assertThat(updated.status()).isEqualTo(TaskStatus.COMPLETED);
                    assertThat(updated.result()).containsEntry("result", "success");
                });

        await().atMost(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    GraphExecution graphExec = graphExecutions.get(executionId);
                    assertThat(graphExec.status()).isEqualTo(GraphStatus.COMPLETED);
                });
    }

    @Test
    void shouldHandleTaskFailure() throws InterruptedException {
        // Given: A graph with task that allows no retries
        Graph graph = Graph.builder("test-graph")
                .tasks(List.of(
                        TaskReference.inline(
                                Task.builder("task1")
                                        .command("echo")
                                        .retry(0)
                                        .build(),
                                List.of()
                        )
                ))
                .build();

        graphDefinitions.put(graph.name(), graph);

        UUID executionId = evaluator.executeGraph("test-graph", Map.of(), "test");

        // Wait for task to be queued (async)
        await().atMost(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    assertThat(workQueue.size()).isGreaterThan(0);
                });

        TaskExecution taskExec = taskExecutions.values().stream()
                .filter(te -> te.graphExecutionId().equals(executionId))
                .findFirst()
                .orElseThrow();

        // When: Task fails (fire event and wait)
        TaskFailureEvent event = TaskFailureEvent.of(taskExec.id(), "Task failed");
        eventBus.send("task.failed", event);

        // Give async processing time
        Thread.sleep(1000);

        // Then: Task marked as failed and graph fails (async)
        await().atMost(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    TaskExecution updated = taskExecutions.get(taskExec.id());
                    assertThat(updated.status()).isEqualTo(TaskStatus.FAILED);
                    assertThat(updated.error()).isEqualTo("Task failed");
                });

        await().atMost(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    GraphExecution graphExec = graphExecutions.get(executionId);
                    assertThat(graphExec.status()).isEqualTo(GraphStatus.FAILED);
                });
    }

    @Test
    void shouldRespectTaskDependencies() throws InterruptedException {
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

        UUID executionId = evaluator.executeGraph("test-graph", Map.of(), "test");

        // Then: Only task-a should be queued initially (async)
        await().atMost(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    assertThat(workQueue.size()).isEqualTo(1);
                });

        WorkMessage work = workQueue.poll();
        assertThat(work.taskName()).isEqualTo("task-a");

        // When: task-a completes
        TaskExecution taskA = taskExecutions.values().stream()
                .filter(te -> te.taskName().equals("task-a"))
                .findFirst()
                .orElseThrow();

        eventBus.send("task.completed", TaskCompletionEvent.of(taskA.id(), Map.of()));

        // Give async processing time
        Thread.sleep(1000);

        // Then: task-b should now be queued (async)
        await().atMost(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    assertThat(workQueue.size()).isEqualTo(1);
                });

        WorkMessage workB = workQueue.poll();
        assertThat(workB.taskName()).isEqualTo("task-b");
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

        // When: Starting execution (async)
        UUID executionId = evaluator.executeGraph(
                "test-graph",
                Map.of("date", "2025-10-18"),
                "test"
        );

        // Then: Global task execution created (async)
        await().atMost(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    assertThat(globalTasks.size()).isEqualTo(1);
                });

        GlobalTaskExecution globalExec = globalTasks.values().iterator().next();
        assertThat(globalExec.taskName()).isEqualTo("load-data");
        assertThat(globalExec.resolvedKey()).isEqualTo("load_2025-10-18");
        assertThat(globalExec.linkedGraphExecutions()).contains(executionId);
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
                .build();

        taskDefinitions.put(globalTask.name(), globalTask);

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

        // When: Two graphs execute with same parameters (async)
        UUID exec1 = evaluator.executeGraph("test-graph", Map.of(), "test");
        UUID exec2 = evaluator.executeGraph("test-graph", Map.of(), "test");

        // Then: Only one global task execution created (async)
        await().atMost(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    assertThat(globalTasks.size()).isEqualTo(1);
                });

        GlobalTaskExecution globalExec = globalTasks.values().iterator().next();
        assertThat(globalExec.linkedGraphExecutions())
                .containsExactlyInAnyOrder(exec1, exec2);

        // And: Only one work message in queue (deduplication worked) (async)
        await().atMost(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    assertThat(workQueue.size()).isEqualTo(1);
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
                                        .args(List.of("Processing ${params.region}"))
                                        .build(),
                                List.of()
                        )
                ))
                .build();

        graphDefinitions.put(graph.name(), graph);

        // When: Starting execution (async)
        evaluator.executeGraph("test-graph", Map.of("region", "eu"), "test");

        // Then: Expression evaluated in work message (async)
        await().atMost(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    assertThat(workQueue.size()).isGreaterThan(0);
                });

        WorkMessage work = workQueue.poll();
        assertThat(work.args()).containsExactly("Processing eu");
    }
}