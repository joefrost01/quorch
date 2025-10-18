package org.neuralchilli.quorch.core;

import jakarta.enterprise.context.ApplicationScoped;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.neuralchilli.quorch.domain.TaskReference;
import org.neuralchilli.quorch.domain.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for building and analyzing DAGs using JGraphT.
 * Provides cycle detection, topological sorting, and ready task identification.
 */
@ApplicationScoped
public class JGraphTService {

    private static final Logger log = LoggerFactory.getLogger(JGraphTService.class);

    /**
     * Build a DAG from a graph definition.
     *
     * @param graph The workflow graph definition
     * @return DirectedAcyclicGraph with TaskNode vertices
     * @throws CycleDetectedException if the graph contains a cycle
     */
    public DirectedAcyclicGraph<TaskNode, DefaultEdge> buildDAG(
            org.neuralchilli.quorch.domain.Graph graph
    ) {
        log.debug("Building DAG for graph: {}", graph.name());

        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag =
                new DirectedAcyclicGraph<>(DefaultEdge.class);

        // First pass: Create all vertices
        Map<String, TaskNode> nodeMap = new HashMap<>();
        for (TaskReference taskRef : graph.tasks()) {
            String taskName = taskRef.getEffectiveName();
            TaskNode node = new TaskNode(
                    taskName,
                    taskRef.isGlobalReference(),
                    taskRef.isGlobalReference() ? taskRef.taskName() : null
            );

            try {
                dag.addVertex(node);
                nodeMap.put(taskName, node);
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Failed to add task '" + taskName + "' to DAG: " + e.getMessage(),
                        e
                );
            }
        }

        // Second pass: Create edges for dependencies
        for (TaskReference taskRef : graph.tasks()) {
            String taskName = taskRef.getEffectiveName();
            TaskNode targetNode = nodeMap.get(taskName);

            for (String dependencyName : taskRef.dependsOn()) {
                TaskNode sourceNode = nodeMap.get(dependencyName);

                if (sourceNode == null) {
                    throw new IllegalStateException(
                            "Task '" + taskName + "' depends on '" + dependencyName +
                                    "' which does not exist in the graph"
                    );
                }

                try {
                    // Edge direction: from dependency to dependent
                    // (dependency must complete before dependent can start)
                    dag.addEdge(sourceNode, targetNode);
                    log.trace("Added edge: {} -> {}", dependencyName, taskName);
                } catch (IllegalArgumentException e) {
                    // JGraphT throws this if adding the edge would create a cycle
                    throw new CycleDetectedException(
                            "Adding dependency '" + dependencyName + "' -> '" + taskName +
                                    "' would create a cycle in the graph"
                    );
                }
            }
        }

        log.debug("DAG built successfully: {} vertices, {} edges",
                dag.vertexSet().size(),
                dag.edgeSet().size()
        );

        return dag;
    }

    /**
     * Find tasks that are ready to execute based on current state.
     * A task is ready if:
     * - It is in PENDING status
     * - All of its dependencies are COMPLETED
     *
     * @param dag The workflow DAG
     * @param currentState Current status of all tasks
     * @return Set of tasks ready to execute
     */
    public Set<TaskNode> findReadyTasks(
            DirectedAcyclicGraph<TaskNode, DefaultEdge> dag,
            Map<TaskNode, TaskStatus> currentState
    ) {
        Set<TaskNode> ready = new HashSet<>();

        for (TaskNode node : dag.vertexSet()) {
            TaskStatus status = currentState.getOrDefault(node, TaskStatus.PENDING);

            // Only consider pending tasks
            if (status != TaskStatus.PENDING) {
                continue;
            }

            // Check if all dependencies are completed
            if (allDependenciesCompleted(dag, node, currentState)) {
                ready.add(node);
            }
        }

        log.debug("Found {} ready tasks", ready.size());
        return ready;
    }

    /**
     * Check if all dependencies of a task are completed.
     */
    private boolean allDependenciesCompleted(
            DirectedAcyclicGraph<TaskNode, DefaultEdge> dag,
            TaskNode node,
            Map<TaskNode, TaskStatus> currentState
    ) {
        Set<DefaultEdge> incomingEdges = dag.incomingEdgesOf(node);

        for (DefaultEdge edge : incomingEdges) {
            TaskNode dependency = dag.getEdgeSource(edge);
            TaskStatus depStatus = currentState.getOrDefault(dependency, TaskStatus.PENDING);

            if (depStatus != TaskStatus.COMPLETED) {
                return false;
            }
        }

        return true;
    }

    /**
     * Get all dependencies of a task (immediate predecessors).
     */
    public Set<TaskNode> getDependencies(
            DirectedAcyclicGraph<TaskNode, DefaultEdge> dag,
            TaskNode node
    ) {
        return dag.incomingEdgesOf(node).stream()
                .map(dag::getEdgeSource)
                .collect(Collectors.toSet());
    }

    /**
     * Get all dependents of a task (immediate successors).
     */
    public Set<TaskNode> getDependents(
            DirectedAcyclicGraph<TaskNode, DefaultEdge> dag,
            TaskNode node
    ) {
        return dag.outgoingEdgesOf(node).stream()
                .map(dag::getEdgeTarget)
                .collect(Collectors.toSet());
    }

    /**
     * Get topological order of all tasks.
     * Tasks earlier in the list must execute before tasks later in the list.
     */
    public List<TaskNode> getTopologicalOrder(
            DirectedAcyclicGraph<TaskNode, DefaultEdge> dag
    ) {
        List<TaskNode> order = new ArrayList<>();
        TopologicalOrderIterator<TaskNode, DefaultEdge> iterator =
                new TopologicalOrderIterator<>(dag);

        while (iterator.hasNext()) {
            order.add(iterator.next());
        }

        return order;
    }

    /**
     * Find all root tasks (tasks with no dependencies).
     */
    public Set<TaskNode> getRootTasks(
            DirectedAcyclicGraph<TaskNode, DefaultEdge> dag
    ) {
        return dag.vertexSet().stream()
                .filter(node -> dag.incomingEdgesOf(node).isEmpty())
                .collect(Collectors.toSet());
    }

    /**
     * Find all leaf tasks (tasks with no dependents).
     */
    public Set<TaskNode> getLeafTasks(
            DirectedAcyclicGraph<TaskNode, DefaultEdge> dag
    ) {
        return dag.vertexSet().stream()
                .filter(node -> dag.outgoingEdgesOf(node).isEmpty())
                .collect(Collectors.toSet());
    }

    /**
     * Check if a task is blocked by failed dependencies.
     * A task is blocked if any of its transitive dependencies have failed.
     */
    public boolean isBlockedByFailure(
            DirectedAcyclicGraph<TaskNode, DefaultEdge> dag,
            TaskNode node,
            Map<TaskNode, TaskStatus> currentState
    ) {
        // BFS through all transitive dependencies
        Queue<TaskNode> queue = new LinkedList<>();
        Set<TaskNode> visited = new HashSet<>();

        queue.addAll(getDependencies(dag, node));

        while (!queue.isEmpty()) {
            TaskNode dependency = queue.poll();

            if (!visited.add(dependency)) {
                continue; // Already visited
            }

            TaskStatus status = currentState.getOrDefault(dependency, TaskStatus.PENDING);

            if (status == TaskStatus.FAILED || status == TaskStatus.SKIPPED) {
                return true;
            }

            // Add transitive dependencies
            queue.addAll(getDependencies(dag, dependency));
        }

        return false;
    }

    /**
     * Calculate the critical path (longest path through the DAG).
     * Returns tasks in the critical path order.
     */
    public List<TaskNode> getCriticalPath(
            DirectedAcyclicGraph<TaskNode, DefaultEdge> dag
    ) {
        // For now, return topological order
        // In future, could consider estimated task durations
        return getTopologicalOrder(dag);
    }

    /**
     * Get execution levels (tasks that can execute in parallel).
     * Each level contains tasks that have no dependencies on each other.
     */
    public List<Set<TaskNode>> getExecutionLevels(
            DirectedAcyclicGraph<TaskNode, DefaultEdge> dag
    ) {
        List<Set<TaskNode>> levels = new ArrayList<>();
        Set<TaskNode> processed = new HashSet<>();
        Set<TaskNode> remaining = new HashSet<>(dag.vertexSet());

        while (!remaining.isEmpty()) {
            Set<TaskNode> currentLevel = new HashSet<>();

            for (TaskNode node : remaining) {
                // Check if all dependencies are processed
                Set<TaskNode> dependencies = getDependencies(dag, node);
                if (processed.containsAll(dependencies)) {
                    currentLevel.add(node);
                }
            }

            if (currentLevel.isEmpty()) {
                // Should not happen in a valid DAG
                throw new IllegalStateException(
                        "Could not determine execution levels - possible cycle or invalid state"
                );
            }

            levels.add(currentLevel);
            processed.addAll(currentLevel);
            remaining.removeAll(currentLevel);
        }

        log.debug("Graph has {} execution levels", levels.size());
        return levels;
    }

    /**
     * Find node by task name.
     */
    public Optional<TaskNode> findNode(
            DirectedAcyclicGraph<TaskNode, DefaultEdge> dag,
            String taskName
    ) {
        return dag.vertexSet().stream()
                .filter(node -> node.taskName().equals(taskName))
                .findFirst();
    }

    /**
     * Get statistics about the DAG.
     */
    public DagStatistics getStatistics(
            DirectedAcyclicGraph<TaskNode, DefaultEdge> dag
    ) {
        int totalTasks = dag.vertexSet().size();
        int rootTasks = getRootTasks(dag).size();
        int leafTasks = getLeafTasks(dag).size();
        int executionLevels = getExecutionLevels(dag).size();
        int maxParallelism = getExecutionLevels(dag).stream()
                .mapToInt(Set::size)
                .max()
                .orElse(0);

        return new DagStatistics(
                totalTasks,
                rootTasks,
                leafTasks,
                executionLevels,
                maxParallelism
        );
    }
}