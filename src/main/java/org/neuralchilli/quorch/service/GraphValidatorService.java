package org.neuralchilli.quorch.service;

import jakarta.enterprise.context.ApplicationScoped;
import org.neuralchilli.quorch.domain.Graph;
import org.neuralchilli.quorch.domain.Task;
import org.neuralchilli.quorch.domain.TaskReference;

import java.util.*;

/**
 * Validates graph and task definitions beyond basic domain validation.
 * Checks for circular dependencies, missing task references, etc.
 */
@ApplicationScoped
public class GraphValidatorService {

    /**
     * Validate a graph definition
     *
     * @throws ValidationException if validation fails
     */
    public void validateGraph(Graph graph) {
        List<String> errors = new ArrayList<>();

        // Validate task references
        validateTaskReferences(graph, errors);

        // Validate no circular dependencies
        validateNoCycles(graph, errors);

        if (!errors.isEmpty()) {
            throw new ValidationException("Graph validation failed for '" + graph.name() + "':\n" +
                    String.join("\n", errors));
        }
    }

    /**
     * Validate a task definition
     *
     * @throws ValidationException if validation fails
     */
    public void validateTask(Task task) {
        List<String> errors = new ArrayList<>();

        // Additional task validation beyond domain rules
        // (Currently domain validation is sufficient, but this is here for future use)

        if (!errors.isEmpty()) {
            throw new ValidationException("Task validation failed for '" + task.name() + "':\n" +
                    String.join("\n", errors));
        }
    }

    private void validateTaskReferences(Graph graph, List<String> errors) {
        Set<String> taskNames = new HashSet<>(graph.getTaskNames());

        for (TaskReference taskRef : graph.tasks()) {
            // Check that all dependencies refer to tasks that exist in this graph
            for (String dependency : taskRef.dependsOn()) {
                if (!taskNames.contains(dependency)) {
                    errors.add("Task '" + taskRef.getEffectiveName() +
                            "' depends on '" + dependency + "' which is not defined in this graph");
                }
            }
        }
    }

    private void validateNoCycles(Graph graph, List<String> errors) {
        // Build adjacency list
        Map<String, List<String>> adjacencyList = new HashMap<>();
        for (TaskReference taskRef : graph.tasks()) {
            String taskName = taskRef.getEffectiveName();
            adjacencyList.put(taskName, new ArrayList<>(taskRef.dependsOn()));
        }

        // Check for cycles using DFS
        Set<String> visited = new HashSet<>();
        Set<String> recursionStack = new HashSet<>();

        for (String taskName : adjacencyList.keySet()) {
            if (hasCycle(taskName, adjacencyList, visited, recursionStack)) {
                errors.add("Circular dependency detected involving task: " + taskName);
                return; // Only report first cycle found
            }
        }
    }

    private boolean hasCycle(
            String taskName,
            Map<String, List<String>> adjacencyList,
            Set<String> visited,
            Set<String> recursionStack
    ) {
        if (recursionStack.contains(taskName)) {
            return true; // Cycle detected
        }

        if (visited.contains(taskName)) {
            return false; // Already processed this path
        }

        visited.add(taskName);
        recursionStack.add(taskName);

        List<String> dependencies = adjacencyList.getOrDefault(taskName, List.of());
        for (String dependency : dependencies) {
            if (hasCycle(dependency, adjacencyList, visited, recursionStack)) {
                return true;
            }
        }

        recursionStack.remove(taskName);
        return false;
    }
}