package org.neuralchilli.quorch.domain;

import java.util.List;
import java.util.Map;

/**
 * Represents a reference to a task within a graph.
 * Can reference a global task or contain an inline task definition.
 */
public record TaskReference(
        String taskName,           // For global task references
        Task inlineTask,           // For inline task definitions
        Map<String, Object> params, // Parameter overrides
        List<String> dependsOn
) {
    public TaskReference {
        if (taskName == null && inlineTask == null) {
            throw new IllegalArgumentException(
                    "TaskReference must have either taskName (global) or inlineTask (inline)"
            );
        }

        if (taskName != null && inlineTask != null) {
            throw new IllegalArgumentException(
                    "TaskReference cannot have both taskName and inlineTask"
            );
        }

        if (params == null) {
            params = Map.of();
        }

        if (dependsOn == null) {
            dependsOn = List.of();
        }
    }

    /**
     * Check if this is a reference to a global task
     */
    public boolean isGlobalReference() {
        return taskName != null;
    }

    /**
     * Check if this is an inline task
     */
    public boolean isInline() {
        return inlineTask != null;
    }

    /**
     * Get the effective task name (either from reference or inline task)
     */
    public String getEffectiveName() {
        return isGlobalReference() ? taskName : inlineTask.name();
    }

    /**
     * Create a reference to a global task
     */
    public static TaskReference toGlobal(String taskName, Map<String, Object> params, List<String> dependsOn) {
        return new TaskReference(taskName, null, params, dependsOn);
    }

    /**
     * Create an inline task reference
     */
    public static TaskReference inline(Task task, List<String> dependsOn) {
        return new TaskReference(null, task, Map.of(), dependsOn);
    }
}