package org.neuralchilli.quorch.worker;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.neuralchilli.quorch.core.ExpressionEvaluator;
import org.neuralchilli.quorch.core.WorkMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Executes task commands with proper environment setup and output capture.
 * Supports trial-run mode for testing without actual execution.
 */
@ApplicationScoped
public class TaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(TaskExecutor.class);

    @ConfigProperty(name = "orchestrator.dev.trial-run", defaultValue = "false")
    boolean trialRun;

    @Inject
    ExpressionEvaluator expressionEvaluator;

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Execute a task work message.
     *
     * @param work Work message containing task details
     * @return Task result with success/failure and output data
     */
    public TaskResult execute(WorkMessage work) {
        // Evaluate all expressions in the work message
        String command = expressionEvaluator.evaluate(work.command(), work.context());

        List<String> args = work.args().stream()
                .map(arg -> expressionEvaluator.evaluate(arg, work.context()))
                .collect(Collectors.toList());

        Map<String, String> env = work.env().entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> expressionEvaluator.evaluate(e.getValue(), work.context())
                ));

        // Build full command
        List<String> fullCommand = new ArrayList<>();
        fullCommand.add(command);
        fullCommand.addAll(args);

        // Trial run mode - log what would execute but don't execute
        if (trialRun) {
            return executeTrialRun(fullCommand, env, work);
        }

        // Real execution
        return executeCommand(fullCommand, env, work);
    }

    /**
     * Trial run mode - log command without executing.
     * Perfect for testing configuration.
     */
    private TaskResult executeTrialRun(
            List<String> command,
            Map<String, String> env,
            WorkMessage work
    ) {
        String commandStr = String.join(" ", command);

        log.info("═══════════════════════════════════════");
        log.info("TRIAL RUN - Would execute:");
        log.info("  Task: {}", work.taskName());
        log.info("  Command: {}", commandStr);
        log.info("  Timeout: {}s", work.timeoutSeconds());
        log.info("  Attempt: {}", work.attempt());
        log.info("  Environment:");
        env.forEach((k, v) -> log.info("    {}={}", k, v));
        log.info("═══════════════════════════════════════");

        // Return fake success
        return TaskResult.success(Map.of(
                "trial_run", true,
                "command", commandStr,
                "timeout", work.timeoutSeconds(),
                "attempt", work.attempt()
        ));
    }

    /**
     * Execute the actual command via ProcessBuilder.
     */
    private TaskResult executeCommand(
            List<String> command,
            Map<String, String> env,
            WorkMessage work
    ) {
        log.debug("Executing command: {}", String.join(" ", command));

        try {
            ProcessBuilder pb = new ProcessBuilder(command);

            // Set environment variables
            pb.environment().putAll(env);

            // Redirect stderr to stdout for unified logging
            pb.redirectErrorStream(true);

            // Start process
            Process process = pb.start();

            // Capture output
            StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                    log.info("[TASK OUTPUT] {}", line);
                }
            }

            // Wait for completion with timeout
            boolean completed = process.waitFor(
                    work.timeoutSeconds(),
                    TimeUnit.SECONDS
            );

            if (!completed) {
                // Timeout - kill process
                process.destroyForcibly();
                return TaskResult.failure(
                        "Task timed out after " + work.timeoutSeconds() + " seconds");
            }

            int exitCode = process.exitValue();

            if (exitCode == 0) {
                // Success - try to parse JSON output for downstream tasks
                Map<String, Object> result = tryParseJsonOutput(output.toString());

                return TaskResult.success(result);
            } else {
                // Non-zero exit code
                return TaskResult.failure(
                        "Task exited with code " + exitCode + "\n" + output.toString());
            }

        } catch (IOException e) {
            return TaskResult.failure(
                    "Failed to start process: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return TaskResult.failure("Task interrupted");
        }
    }

    /**
     * Try to parse the last line of output as JSON.
     * This allows tasks to pass structured data to downstream tasks.
     *
     * If the last line is valid JSON, return it as a map.
     * Otherwise, return the full output as a string.
     */
    private Map<String, Object> tryParseJsonOutput(String output) {
        try {
            String[] lines = output.split("\n");
            if (lines.length == 0) {
                return Map.of("output", output);
            }

            String lastLine = lines[lines.length - 1].trim();

            // Check if last line looks like JSON
            if (lastLine.startsWith("{") && lastLine.endsWith("}")) {
                Map<String, Object> parsed = objectMapper.readValue(
                        lastLine,
                        new TypeReference<Map<String, Object>>() {}
                );
                return parsed;
            }
        } catch (Exception e) {
            // Not JSON, that's fine
            log.trace("Last line is not valid JSON: {}", e.getMessage());
        }

        // Return output as string
        return Map.of("output", output.trim());
    }
}