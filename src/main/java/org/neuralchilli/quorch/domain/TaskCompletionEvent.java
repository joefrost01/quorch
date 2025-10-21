package org.neuralchilli.quorch.domain;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

/**
 * Event published when a task completes successfully.
 */
public final class TaskCompletionEvent implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final UUID taskExecutionId;
    private final Map<String, Object> result;

    public TaskCompletionEvent(UUID taskExecutionId, Map<String, Object> result) {
        if (taskExecutionId == null) {
            throw new IllegalArgumentException("Task execution ID cannot be null");
        }
        this.taskExecutionId = taskExecutionId;
        this.result = result != null ? Map.copyOf(result) : Map.of();
    }

    public UUID taskExecutionId() {
        return taskExecutionId;
    }

    public Map<String, Object> result() {
        return result;
    }

    public static TaskCompletionEvent of(UUID taskExecutionId, Map<String, Object> result) {
        return new TaskCompletionEvent(taskExecutionId, result);
    }

    @Override
    public String toString() {
        return "TaskCompletionEvent[taskExecutionId=" + taskExecutionId + ", result=" + result + "]";
    }
}