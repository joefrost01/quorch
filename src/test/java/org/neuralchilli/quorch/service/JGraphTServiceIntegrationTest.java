package org.neuralchilli.quorch.service;

import com.hazelcast.core.HazelcastInstance;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neuralchilli.quorch.domain.DagStatistics;
import org.neuralchilli.quorch.domain.Graph;
import org.neuralchilli.quorch.domain.TaskNode;
import org.neuralchilli.quorch.domain.TaskStatus;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
class JGraphTServiceIntegrationTest {

    @Inject
    JGraphTService jGraphTService;

    @Inject
    GraphLoaderService graphLoaderService;

    @Inject
    HazelcastInstance hazelcast;

    private Path tempDir;

    @BeforeEach
    void setup() throws IOException {
        tempDir = Files.createTempDirectory("quorch-integration-test-");
    }

    @AfterEach
    void cleanup() throws IOException {
        if (tempDir != null && Files.exists(tempDir)) {
            Files.walk(tempDir)
                    .sorted((a, b) -> -a.compareTo(b))
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            // Ignore
                        }
                    });
        }

        // Clear Hazelcast maps
        hazelcast.getMap("graph-definitions").clear();
        hazelcast.getMap("task-definitions").clear();
    }

    @Test
    void shouldBuildDAGFromLoadedGraph() throws IOException {
        // Given: A complex graph loaded from YAML
        String graphYaml = """
                name: etl-pipeline
                description: "ETL pipeline with multiple stages"
                tasks:
                  - name: extract
                    command: python
                    args:
                      - extract.py
                  - name: transform
                    command: python
                    args:
                      - transform.py
                    depends_on:
                      - extract
                  - name: validate
                    command: python
                    args:
                      - validate.py
                    depends_on:
                      - extract
                  - name: load
                    command: python
                    args:
                      - load.py
                    depends_on:
                      - transform
                      - validate
                """;

        Path graphFile = tempDir.resolve("etl-pipeline.yaml");
        Files.writeString(graphFile, graphYaml);

        LoadResult result = graphLoaderService.loadGraph(graphFile);
        assertThat(result.isSuccess()).isTrue();

        // Retrieve the loaded graph
        Graph graph = (Graph) hazelcast.getMap("graph-definitions").get("etl-pipeline");
        assertThat(graph).isNotNull();

        // When: Building DAG from loaded graph
        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = jGraphTService.buildDAG(graph);

        // Then: DAG structure matches YAML definition
        assertThat(dag.vertexSet()).hasSize(4);

        // Verify execution levels
        List<Set<TaskNode>> levels = jGraphTService.getExecutionLevels(dag);
        assertThat(levels).hasSize(3);

        // Level 0: extract (root)
        assertThat(levels.get(0)).extracting(TaskNode::taskName)
                .containsExactly("extract");

        // Level 1: transform, validate (parallel)
        assertThat(levels.get(1)).extracting(TaskNode::taskName)
                .containsExactlyInAnyOrder("transform", "validate");

        // Level 2: load (leaf)
        assertThat(levels.get(2)).extracting(TaskNode::taskName)
                .containsExactly("load");
    }

    @Test
    void shouldSimulateGraphExecution() throws IOException {
        // Given: A simple graph
        String graphYaml = """
                name: simple-workflow
                tasks:
                  - name: step1
                    command: echo
                    args:
                      - "Step 1"
                  - name: step2
                    command: echo
                    args:
                      - "Step 2"
                    depends_on:
                      - step1
                  - name: step3
                    command: echo
                    args:
                      - "Step 3"
                    depends_on:
                      - step2
                """;

        Path graphFile = tempDir.resolve("simple-workflow.yaml");
        Files.writeString(graphFile, graphYaml);

        graphLoaderService.loadGraph(graphFile);
        Graph graph = (Graph) hazelcast.getMap("graph-definitions").get("simple-workflow");

        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = jGraphTService.buildDAG(graph);

        // Simulate execution
        Map<TaskNode, TaskStatus> state = new java.util.HashMap<>();

        TaskNode step1 = jGraphTService.findNode(dag, "step1").orElseThrow();
        TaskNode step2 = jGraphTService.findNode(dag, "step2").orElseThrow();
        TaskNode step3 = jGraphTService.findNode(dag, "step3").orElseThrow();

        // Initially all pending
        state.put(step1, TaskStatus.PENDING);
        state.put(step2, TaskStatus.PENDING);
        state.put(step3, TaskStatus.PENDING);

        // Round 1: Only step1 should be ready
        Set<TaskNode> ready1 = jGraphTService.findReadyTasks(dag, state);
        assertThat(ready1).containsExactly(step1);

        // Complete step1
        state.put(step1, TaskStatus.COMPLETED);

        // Round 2: Only step2 should be ready
        Set<TaskNode> ready2 = jGraphTService.findReadyTasks(dag, state);
        assertThat(ready2).containsExactly(step2);

        // Complete step2
        state.put(step2, TaskStatus.COMPLETED);

        // Round 3: Only step3 should be ready
        Set<TaskNode> ready3 = jGraphTService.findReadyTasks(dag, state);
        assertThat(ready3).containsExactly(step3);

        // Complete step3
        state.put(step3, TaskStatus.COMPLETED);

        // Round 4: No more tasks ready
        Set<TaskNode> ready4 = jGraphTService.findReadyTasks(dag, state);
        assertThat(ready4).isEmpty();
    }

    @Test
    void shouldHandleParallelExecution() throws IOException {
        // Given: A graph with parallel branches
        String graphYaml = """
                name: parallel-workflow
                tasks:
                  - name: start
                    command: echo
                    args:
                      - "Start"
                  - name: branch-a
                    command: echo
                    args:
                      - "Branch A"
                    depends_on:
                      - start
                  - name: branch-b
                    command: echo
                    args:
                      - "Branch B"
                    depends_on:
                      - start
                  - name: branch-c
                    command: echo
                    args:
                      - "Branch C"
                    depends_on:
                      - start
                  - name: join
                    command: echo
                    args:
                      - "Join"
                    depends_on:
                      - branch-a
                      - branch-b
                      - branch-c
                """;

        Path graphFile = tempDir.resolve("parallel-workflow.yaml");
        Files.writeString(graphFile, graphYaml);

        graphLoaderService.loadGraph(graphFile);
        Graph graph = (Graph) hazelcast.getMap("graph-definitions").get("parallel-workflow");

        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = jGraphTService.buildDAG(graph);

        // Get statistics
        DagStatistics stats = jGraphTService.getStatistics(dag);
        assertThat(stats.totalTasks()).isEqualTo(5);
        assertThat(stats.maxParallelism()).isEqualTo(3); // 3 branches in parallel
        assertThat(stats.executionLevels()).isEqualTo(3); // start -> branches -> join
        assertThat(stats.hasParallelism()).isTrue();

        // Simulate execution
        Map<TaskNode, TaskStatus> state = new java.util.HashMap<>();

        TaskNode start = jGraphTService.findNode(dag, "start").orElseThrow();
        TaskNode branchA = jGraphTService.findNode(dag, "branch-a").orElseThrow();
        TaskNode branchB = jGraphTService.findNode(dag, "branch-b").orElseThrow();
        TaskNode branchC = jGraphTService.findNode(dag, "branch-c").orElseThrow();
        TaskNode join = jGraphTService.findNode(dag, "join").orElseThrow();

        // All pending initially
        for (TaskNode node : dag.vertexSet()) {
            state.put(node, TaskStatus.PENDING);
        }

        // Round 1: Only start is ready
        Set<TaskNode> ready1 = jGraphTService.findReadyTasks(dag, state);
        assertThat(ready1).containsExactly(start);

        // Complete start
        state.put(start, TaskStatus.COMPLETED);

        // Round 2: All three branches ready (parallel execution)
        Set<TaskNode> ready2 = jGraphTService.findReadyTasks(dag, state);
        assertThat(ready2).containsExactlyInAnyOrder(branchA, branchB, branchC);

        // Complete all branches
        state.put(branchA, TaskStatus.COMPLETED);
        state.put(branchB, TaskStatus.COMPLETED);
        state.put(branchC, TaskStatus.COMPLETED);

        // Round 3: Join is ready
        Set<TaskNode> ready3 = jGraphTService.findReadyTasks(dag, state);
        assertThat(ready3).containsExactly(join);
    }

    @Test
    void shouldHandleFailureBlocking() throws IOException {
        // Given: A graph where failure blocks downstream
        String graphYaml = """
                name: failure-test
                tasks:
                  - name: task-a
                    command: echo
                    args:
                      - "A"
                  - name: task-b
                    command: echo
                    args:
                      - "B"
                    depends_on:
                      - task-a
                  - name: task-c
                    command: echo
                    args:
                      - "C"
                    depends_on:
                      - task-b
                """;

        Path graphFile = tempDir.resolve("failure-test.yaml");
        Files.writeString(graphFile, graphYaml);

        graphLoaderService.loadGraph(graphFile);
        Graph graph = (Graph) hazelcast.getMap("graph-definitions").get("failure-test");

        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = jGraphTService.buildDAG(graph);

        TaskNode taskA = jGraphTService.findNode(dag, "task-a").orElseThrow();
        TaskNode taskB = jGraphTService.findNode(dag, "task-b").orElseThrow();
        TaskNode taskC = jGraphTService.findNode(dag, "task-c").orElseThrow();

        // Simulate task-a failing
        Map<TaskNode, TaskStatus> state = Map.of(
                taskA, TaskStatus.FAILED,
                taskB, TaskStatus.PENDING,
                taskC, TaskStatus.PENDING
        );

        // Both B and C should be blocked by A's failure
        assertThat(jGraphTService.isBlockedByFailure(dag, taskB, state)).isTrue();
        assertThat(jGraphTService.isBlockedByFailure(dag, taskC, state)).isTrue();

        // No tasks should be ready
        Set<TaskNode> ready = jGraphTService.findReadyTasks(dag, state);
        assertThat(ready).isEmpty();
    }

    @Test
    void shouldGetTopologicalOrderForComplexGraph() throws IOException {
        // Given: A complex DAG
        String graphYaml = """
                name: complex-dag
                tasks:
                  - name: a
                    command: echo
                  - name: b
                    command: echo
                    depends_on: [a]
                  - name: c
                    command: echo
                    depends_on: [a]
                  - name: d
                    command: echo
                    depends_on: [b]
                  - name: e
                    command: echo
                    depends_on: [b, c]
                  - name: f
                    command: echo
                    depends_on: [d, e]
                """;

        Path graphFile = tempDir.resolve("complex-dag.yaml");
        Files.writeString(graphFile, graphYaml);

        graphLoaderService.loadGraph(graphFile);
        Graph graph = (Graph) hazelcast.getMap("graph-definitions").get("complex-dag");

        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = jGraphTService.buildDAG(graph);

        // Get topological order
        List<TaskNode> order = jGraphTService.getTopologicalOrder(dag);

        assertThat(order).hasSize(6);

        // Verify ordering constraints
        int indexA = findIndex(order, "a");
        int indexB = findIndex(order, "b");
        int indexC = findIndex(order, "c");
        int indexD = findIndex(order, "d");
        int indexE = findIndex(order, "e");
        int indexF = findIndex(order, "f");

        // A must come before B and C
        assertThat(indexA).isLessThan(indexB);
        assertThat(indexA).isLessThan(indexC);

        // B must come before D and E
        assertThat(indexB).isLessThan(indexD);
        assertThat(indexB).isLessThan(indexE);

        // C must come before E
        assertThat(indexC).isLessThan(indexE);

        // D and E must come before F
        assertThat(indexD).isLessThan(indexF);
        assertThat(indexE).isLessThan(indexF);
    }

    private int findIndex(List<TaskNode> list, String taskName) {
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).taskName().equals(taskName)) {
                return i;
            }
        }
        return -1;
    }
}