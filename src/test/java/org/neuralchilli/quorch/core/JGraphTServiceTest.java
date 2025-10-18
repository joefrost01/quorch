package org.neuralchilli.quorch.core;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.junit.jupiter.api.Test;
import org.neuralchilli.quorch.domain.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

@QuarkusTest
class JGraphTServiceTest {

    @Inject
    JGraphTService service;

    @Test
    void shouldBuildSimpleLinearDAG() {
        // Given: A -> B -> C
        Graph graph = Graph.builder("linear-graph")
                .tasks(List.of(
                        TaskReference.inline(
                                Task.builder("task-a").command("echo").build(),
                                List.of()
                        ),
                        TaskReference.inline(
                                Task.builder("task-b").command("echo").build(),
                                List.of("task-a")
                        ),
                        TaskReference.inline(
                                Task.builder("task-c").command("echo").build(),
                                List.of("task-b")
                        )
                ))
                .build();

        // When: Building DAG
        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = service.buildDAG(graph);

        // Then: DAG structure is correct
        assertThat(dag.vertexSet()).hasSize(3);
        assertThat(dag.edgeSet()).hasSize(2);

        // Verify topology
        List<TaskNode> order = service.getTopologicalOrder(dag);
        assertThat(order).extracting(TaskNode::taskName)
                .containsExactly("task-a", "task-b", "task-c");
    }

    @Test
    void shouldBuildDiamondDAG() {
        // Given: A -> B -> D
        //           -> C ->
        Graph graph = Graph.builder("diamond-graph")
                .tasks(List.of(
                        TaskReference.inline(
                                Task.builder("task-a").command("echo").build(),
                                List.of()
                        ),
                        TaskReference.inline(
                                Task.builder("task-b").command("echo").build(),
                                List.of("task-a")
                        ),
                        TaskReference.inline(
                                Task.builder("task-c").command("echo").build(),
                                List.of("task-a")
                        ),
                        TaskReference.inline(
                                Task.builder("task-d").command("echo").build(),
                                List.of("task-b", "task-c")
                        )
                ))
                .build();

        // When: Building DAG
        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = service.buildDAG(graph);

        // Then: DAG structure is correct
        assertThat(dag.vertexSet()).hasSize(4);
        assertThat(dag.edgeSet()).hasSize(4); // A->B, A->C, B->D, C->D

        // Verify root and leaf tasks
        Set<TaskNode> roots = service.getRootTasks(dag);
        assertThat(roots).extracting(TaskNode::taskName).containsExactly("task-a");

        Set<TaskNode> leaves = service.getLeafTasks(dag);
        assertThat(leaves).extracting(TaskNode::taskName).containsExactly("task-d");
    }

    @Test
    void shouldDetectCycle() {
        // Given: A -> B -> C -> A (cycle)
        Graph graph = Graph.builder("cyclic-graph")
                .tasks(List.of(
                        TaskReference.inline(
                                Task.builder("task-a").command("echo").build(),
                                List.of("task-c")
                        ),
                        TaskReference.inline(
                                Task.builder("task-b").command("echo").build(),
                                List.of("task-a")
                        ),
                        TaskReference.inline(
                                Task.builder("task-c").command("echo").build(),
                                List.of("task-b")
                        )
                ))
                .build();

        // When/Then: Building DAG should throw
        assertThatThrownBy(() -> service.buildDAG(graph))
                .isInstanceOf(CycleDetectedException.class)
                .hasMessageContaining("cycle");
    }

    @Test
    void shouldDetectSelfReference() {
        // Given: A -> A (self-reference)
        Graph graph = Graph.builder("self-ref-graph")
                .tasks(List.of(
                        TaskReference.inline(
                                Task.builder("task-a").command("echo").build(),
                                List.of("task-a")
                        )
                ))
                .build();

        // When/Then: Building DAG should throw
        assertThatThrownBy(() -> service.buildDAG(graph))
                .isInstanceOf(CycleDetectedException.class);
    }

    @Test
    void shouldFindReadyTasksInLinearGraph() {
        // Given: A -> B -> C, where A is completed
        Graph graph = Graph.builder("linear")
                .tasks(List.of(
                        TaskReference.inline(Task.builder("task-a").command("echo").build(), List.of()),
                        TaskReference.inline(Task.builder("task-b").command("echo").build(), List.of("task-a")),
                        TaskReference.inline(Task.builder("task-c").command("echo").build(), List.of("task-b"))
                ))
                .build();

        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = service.buildDAG(graph);

        TaskNode nodeA = service.findNode(dag, "task-a").orElseThrow();
        TaskNode nodeB = service.findNode(dag, "task-b").orElseThrow();
        TaskNode nodeC = service.findNode(dag, "task-c").orElseThrow();

        Map<TaskNode, TaskStatus> state = Map.of(
                nodeA, TaskStatus.COMPLETED,
                nodeB, TaskStatus.PENDING,
                nodeC, TaskStatus.PENDING
        );

        // When: Finding ready tasks
        Set<TaskNode> ready = service.findReadyTasks(dag, state);

        // Then: Only B is ready
        assertThat(ready).containsExactly(nodeB);
    }

    @Test
    void shouldFindMultipleReadyTasksInDiamond() {
        // Given: A -> B -> D
        //           -> C ->
        // Where A is completed, B and C should both be ready
        Graph graph = Graph.builder("diamond")
                .tasks(List.of(
                        TaskReference.inline(Task.builder("task-a").command("echo").build(), List.of()),
                        TaskReference.inline(Task.builder("task-b").command("echo").build(), List.of("task-a")),
                        TaskReference.inline(Task.builder("task-c").command("echo").build(), List.of("task-a")),
                        TaskReference.inline(Task.builder("task-d").command("echo").build(), List.of("task-b", "task-c"))
                ))
                .build();

        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = service.buildDAG(graph);

        TaskNode nodeA = service.findNode(dag, "task-a").orElseThrow();
        TaskNode nodeB = service.findNode(dag, "task-b").orElseThrow();
        TaskNode nodeC = service.findNode(dag, "task-c").orElseThrow();
        TaskNode nodeD = service.findNode(dag, "task-d").orElseThrow();

        Map<TaskNode, TaskStatus> state = Map.of(
                nodeA, TaskStatus.COMPLETED,
                nodeB, TaskStatus.PENDING,
                nodeC, TaskStatus.PENDING,
                nodeD, TaskStatus.PENDING
        );

        // When: Finding ready tasks
        Set<TaskNode> ready = service.findReadyTasks(dag, state);

        // Then: Both B and C are ready (parallel execution possible)
        assertThat(ready).containsExactlyInAnyOrder(nodeB, nodeC);
    }

    @Test
    void shouldNotReturnRunningTasksAsReady() {
        Graph graph = Graph.builder("test")
                .tasks(List.of(
                        TaskReference.inline(Task.builder("task-a").command("echo").build(), List.of())
                ))
                .build();

        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = service.buildDAG(graph);
        TaskNode nodeA = service.findNode(dag, "task-a").orElseThrow();

        Map<TaskNode, TaskStatus> state = Map.of(
                nodeA, TaskStatus.RUNNING
        );

        // When: Finding ready tasks
        Set<TaskNode> ready = service.findReadyTasks(dag, state);

        // Then: No tasks are ready
        assertThat(ready).isEmpty();
    }

    @Test
    void shouldGetDependencies() {
        // Given: A -> B -> D
        //           -> C ->
        Graph graph = Graph.builder("diamond")
                .tasks(List.of(
                        TaskReference.inline(Task.builder("task-a").command("echo").build(), List.of()),
                        TaskReference.inline(Task.builder("task-b").command("echo").build(), List.of("task-a")),
                        TaskReference.inline(Task.builder("task-c").command("echo").build(), List.of("task-a")),
                        TaskReference.inline(Task.builder("task-d").command("echo").build(), List.of("task-b", "task-c"))
                ))
                .build();

        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = service.buildDAG(graph);
        TaskNode nodeD = service.findNode(dag, "task-d").orElseThrow();

        // When: Getting dependencies
        Set<TaskNode> dependencies = service.getDependencies(dag, nodeD);

        // Then: D depends on B and C
        assertThat(dependencies).extracting(TaskNode::taskName)
                .containsExactlyInAnyOrder("task-b", "task-c");
    }

    @Test
    void shouldGetDependents() {
        // Given: A -> B -> D
        //           -> C ->
        Graph graph = Graph.builder("diamond")
                .tasks(List.of(
                        TaskReference.inline(Task.builder("task-a").command("echo").build(), List.of()),
                        TaskReference.inline(Task.builder("task-b").command("echo").build(), List.of("task-a")),
                        TaskReference.inline(Task.builder("task-c").command("echo").build(), List.of("task-a")),
                        TaskReference.inline(Task.builder("task-d").command("echo").build(), List.of("task-b", "task-c"))
                ))
                .build();

        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = service.buildDAG(graph);
        TaskNode nodeA = service.findNode(dag, "task-a").orElseThrow();

        // When: Getting dependents
        Set<TaskNode> dependents = service.getDependents(dag, nodeA);

        // Then: A has B and C as dependents
        assertThat(dependents).extracting(TaskNode::taskName)
                .containsExactlyInAnyOrder("task-b", "task-c");
    }

    @Test
    void shouldGetExecutionLevels() {
        // Given: A -> B -> D
        //           -> C ->
        Graph graph = Graph.builder("diamond")
                .tasks(List.of(
                        TaskReference.inline(Task.builder("task-a").command("echo").build(), List.of()),
                        TaskReference.inline(Task.builder("task-b").command("echo").build(), List.of("task-a")),
                        TaskReference.inline(Task.builder("task-c").command("echo").build(), List.of("task-a")),
                        TaskReference.inline(Task.builder("task-d").command("echo").build(), List.of("task-b", "task-c"))
                ))
                .build();

        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = service.buildDAG(graph);

        // When: Getting execution levels
        List<Set<TaskNode>> levels = service.getExecutionLevels(dag);

        // Then: 3 levels: [A], [B, C], [D]
        assertThat(levels).hasSize(3);

        assertThat(levels.get(0)).extracting(TaskNode::taskName)
                .containsExactly("task-a");

        assertThat(levels.get(1)).extracting(TaskNode::taskName)
                .containsExactlyInAnyOrder("task-b", "task-c");

        assertThat(levels.get(2)).extracting(TaskNode::taskName)
                .containsExactly("task-d");
    }

    @Test
    void shouldDetectBlockedByFailure() {
        // Given: A -> B -> C where A failed
        Graph graph = Graph.builder("linear")
                .tasks(List.of(
                        TaskReference.inline(Task.builder("task-a").command("echo").build(), List.of()),
                        TaskReference.inline(Task.builder("task-b").command("echo").build(), List.of("task-a")),
                        TaskReference.inline(Task.builder("task-c").command("echo").build(), List.of("task-b"))
                ))
                .build();

        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = service.buildDAG(graph);

        TaskNode nodeA = service.findNode(dag, "task-a").orElseThrow();
        TaskNode nodeB = service.findNode(dag, "task-b").orElseThrow();
        TaskNode nodeC = service.findNode(dag, "task-c").orElseThrow();

        Map<TaskNode, TaskStatus> state = Map.of(
                nodeA, TaskStatus.FAILED,
                nodeB, TaskStatus.PENDING,
                nodeC, TaskStatus.PENDING
        );

        // When/Then: Both B and C should be blocked
        assertThat(service.isBlockedByFailure(dag, nodeB, state)).isTrue();
        assertThat(service.isBlockedByFailure(dag, nodeC, state)).isTrue();
    }

    @Test
    void shouldNotBeBlockedIfDependenciesSucceed() {
        // Given: A -> B where A completed
        Graph graph = Graph.builder("simple")
                .tasks(List.of(
                        TaskReference.inline(Task.builder("task-a").command("echo").build(), List.of()),
                        TaskReference.inline(Task.builder("task-b").command("echo").build(), List.of("task-a"))
                ))
                .build();

        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = service.buildDAG(graph);

        TaskNode nodeA = service.findNode(dag, "task-a").orElseThrow();
        TaskNode nodeB = service.findNode(dag, "task-b").orElseThrow();

        Map<TaskNode, TaskStatus> state = Map.of(
                nodeA, TaskStatus.COMPLETED,
                nodeB, TaskStatus.PENDING
        );

        // When/Then: B should not be blocked
        assertThat(service.isBlockedByFailure(dag, nodeB, state)).isFalse();
    }

    @Test
    void shouldGetStatistics() {
        // Given: A -> B -> D
        //           -> C ->
        Graph graph = Graph.builder("diamond")
                .tasks(List.of(
                        TaskReference.inline(Task.builder("task-a").command("echo").build(), List.of()),
                        TaskReference.inline(Task.builder("task-b").command("echo").build(), List.of("task-a")),
                        TaskReference.inline(Task.builder("task-c").command("echo").build(), List.of("task-a")),
                        TaskReference.inline(Task.builder("task-d").command("echo").build(), List.of("task-b", "task-c"))
                ))
                .build();

        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = service.buildDAG(graph);

        // When: Getting statistics
        DagStatistics stats = service.getStatistics(dag);

        // Then: Statistics are correct
        assertThat(stats.totalTasks()).isEqualTo(4);
        assertThat(stats.rootTasks()).isEqualTo(1);
        assertThat(stats.leafTasks()).isEqualTo(1);
        assertThat(stats.executionLevels()).isEqualTo(3);
        assertThat(stats.maxParallelism()).isEqualTo(2); // B and C can run in parallel
        assertThat(stats.hasParallelism()).isTrue();
        assertThat(stats.isLinear()).isFalse();
    }

    @Test
    void shouldGetStatisticsForLinearGraph() {
        // Given: A -> B -> C
        Graph graph = Graph.builder("linear")
                .tasks(List.of(
                        TaskReference.inline(Task.builder("task-a").command("echo").build(), List.of()),
                        TaskReference.inline(Task.builder("task-b").command("echo").build(), List.of("task-a")),
                        TaskReference.inline(Task.builder("task-c").command("echo").build(), List.of("task-b"))
                ))
                .build();

        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = service.buildDAG(graph);

        // When: Getting statistics
        DagStatistics stats = service.getStatistics(dag);

        // Then: Linear graph statistics
        assertThat(stats.totalTasks()).isEqualTo(3);
        assertThat(stats.executionLevels()).isEqualTo(3);
        assertThat(stats.maxParallelism()).isEqualTo(1);
        assertThat(stats.hasParallelism()).isFalse();
        assertThat(stats.isLinear()).isTrue();
    }

    @Test
    void shouldHandleGraphWithMultipleRoots() {
        // Given: A -> C
        //        B -> C
        Graph graph = Graph.builder("multi-root")
                .tasks(List.of(
                        TaskReference.inline(Task.builder("task-a").command("echo").build(), List.of()),
                        TaskReference.inline(Task.builder("task-b").command("echo").build(), List.of()),
                        TaskReference.inline(Task.builder("task-c").command("echo").build(), List.of("task-a", "task-b"))
                ))
                .build();

        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = service.buildDAG(graph);

        // When: Getting roots
        Set<TaskNode> roots = service.getRootTasks(dag);

        // Then: Both A and B are roots
        assertThat(roots).extracting(TaskNode::taskName)
                .containsExactlyInAnyOrder("task-a", "task-b");
    }

    @Test
    void shouldFindNodeByName() {
        Graph graph = Graph.builder("test")
                .tasks(List.of(
                        TaskReference.inline(Task.builder("task-a").command("echo").build(), List.of()),
                        TaskReference.inline(Task.builder("task-b").command("echo").build(), List.of("task-a"))
                ))
                .build();

        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = service.buildDAG(graph);

        // When: Finding node
        var nodeA = service.findNode(dag, "task-a");
        var nodeB = service.findNode(dag, "task-b");
        var nodeC = service.findNode(dag, "task-c");

        // Then: Nodes found/not found correctly
        assertThat(nodeA).isPresent();
        assertThat(nodeA.get().taskName()).isEqualTo("task-a");

        assertThat(nodeB).isPresent();
        assertThat(nodeB.get().taskName()).isEqualTo("task-b");

        assertThat(nodeC).isEmpty();
    }

    @Test
    void shouldHandleGlobalTaskReference() {
        Graph graph = Graph.builder("with-global")
                .tasks(List.of(
                        TaskReference.toGlobal("load-data", Map.of("date", "2025-10-17"), List.of()),
                        TaskReference.inline(
                                Task.builder("process").command("echo").build(),
                                List.of("load-data")
                        )
                ))
                .build();

        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = service.buildDAG(graph);

        // When: Building DAG with global reference
        TaskNode globalNode = service.findNode(dag, "load-data").orElseThrow();

        // Then: Global node is marked correctly
        assertThat(globalNode.isGlobal()).isTrue();
        assertThat(globalNode.taskName()).isEqualTo("load-data");
    }

    @Test
    void shouldRejectMissingDependency() {
        // Given: Task B depends on non-existent task A
        Graph graph = Graph.builder("invalid")
                .tasks(List.of(
                        TaskReference.inline(
                                Task.builder("task-b").command("echo").build(),
                                List.of("task-a") // task-a doesn't exist
                        )
                ))
                .build();

        // When/Then: Building DAG should throw
        assertThatThrownBy(() -> service.buildDAG(graph))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("depends on 'task-a'")
                .hasMessageContaining("does not exist");
    }
}