package org.neuralchilli.quorch.domain;

import java.io.Serial;
import java.io.Serializable;
import java.util.UUID;

/**
 * Event published when a task fails.
 */
public final class TaskFailureEvent implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final UUID taskExecutionId;
    private final String error;

    public TaskFailureEvent(UUID taskExecutionId, String error) {
        if (taskExecutionId == null) {
            throw new IllegalArgumentException("Task execution ID cannot be null");
        }
        this.taskExecutionId = taskExecutionId;
        this.error = error;
    }

    public UUID taskExecutionId() {
        return taskExecutionId;
    }

    public String error() {
        return error;
    }

    public static TaskFailureEvent of(UUID taskExecutionId, String error) {
        return new TaskFailureEvent(taskExecutionId, error);
    }

    @Override
    public String toString() {
        return "TaskFailureEvent[taskExecutionId=" + taskExecutionId + ", error=" + error + "]";
    }
}