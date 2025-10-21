package org.neuralchilli.quorch.domain;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a reference to a task within a graph.
 * Can reference a global task or contain an inline task definition.
 */
public final class TaskReference implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String taskName;
    private final Task inlineTask;
    private final Map<String, Object> params;
    private final List<String> dependsOn;

    public TaskReference(
            String taskName,
            Task inlineTask,
            Map<String, Object> params,
            List<String> dependsOn
    ) {
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

        this.taskName = taskName;
        this.inlineTask = inlineTask;
        this.params = params != null ? Map.copyOf(params) : Map.of();
        this.dependsOn = dependsOn != null ? List.copyOf(dependsOn) : List.of();
    }

    // Getters
    public String taskName() {
        return taskName;
    }

    public Task inlineTask() {
        return inlineTask;
    }

    public Map<String, Object> params() {
        return params;
    }

    public List<String> dependsOn() {
        return dependsOn;
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

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        TaskReference that = (TaskReference) obj;
        return Objects.equals(this.taskName, that.taskName) &&
                Objects.equals(this.inlineTask, that.inlineTask) &&
                Objects.equals(this.params, that.params) &&
                Objects.equals(this.dependsOn, that.dependsOn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskName, inlineTask, params, dependsOn);
    }

    @Override
    public String toString() {
        return "TaskReference[" +
                "taskName=" + taskName + ", " +
                "inlineTask=" + inlineTask + ", " +
                "params=" + params + ", " +
                "dependsOn=" + dependsOn + ']';
    }
}