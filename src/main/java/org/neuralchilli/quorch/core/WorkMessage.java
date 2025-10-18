package org.neuralchilli.quorch.core;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Message sent to worker queue containing task execution details.
 * Workers pull these messages and execute the commands.
 */
public final class WorkMessage implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final UUID taskExecutionId;
    private final String taskName;
    private final String command;
    private final List<String> args;
    private final Map<String, String> env;
    private final int timeoutSeconds;
    private final int attempt;
    private final ExpressionContext context;

    public WorkMessage(
            UUID taskExecutionId,
            String taskName,
            String command,
            List<String> args,
            Map<String, String> env,
            int timeoutSeconds,
            int attempt,
            ExpressionContext context
    ) {
        if (taskExecutionId == null) {
            throw new IllegalArgumentException("Task execution ID cannot be null");
        }
        if (taskName == null || taskName.isBlank()) {
            throw new IllegalArgumentException("Task name cannot be null or empty");
        }
        if (command == null || command.isBlank()) {
            throw new IllegalArgumentException("Command cannot be null or empty");
        }

        this.taskExecutionId = taskExecutionId;
        this.taskName = taskName;
        this.command = command;
        this.args = args != null ? List.copyOf(args) : List.of();
        this.env = env != null ? Map.copyOf(env) : Map.of();
        this.timeoutSeconds = timeoutSeconds;
        this.attempt = attempt;
        this.context = context;
    }

    public UUID taskExecutionId() {
        return taskExecutionId;
    }

    public String taskName() {
        return taskName;
    }

    public String command() {
        return command;
    }

    public List<String> args() {
        return args;
    }

    public Map<String, String> env() {
        return env;
    }

    public int timeoutSeconds() {
        return timeoutSeconds;
    }

    public int attempt() {
        return attempt;
    }

    public ExpressionContext context() {
        return context;
    }

    @Override
    public String toString() {
        return "WorkMessage[" +
                "taskExecutionId=" + taskExecutionId +
                ", taskName=" + taskName +
                ", command=" + command +
                ", args=" + args +
                ", timeout=" + timeoutSeconds +
                ", attempt=" + attempt +
                "]";
    }
}