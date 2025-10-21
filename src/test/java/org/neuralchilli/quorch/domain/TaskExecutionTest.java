package org.neuralchilli.quorch.domain;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskExecutionTest {

    @Test
    void shouldCreateNewExecution() {
        UUID graphExecId = UUID.randomUUID();
        TaskExecution exec = TaskExecution.create(
                graphExecId,
                "test-task",
                false,
                null
        );

        assertThat(exec.id()).isNotNull();
        assertThat(exec.graphExecutionId()).isEqualTo(graphExecId);
        assertThat(exec.taskName()).isEqualTo("test-task");
        assertThat(exec.status()).isEqualTo(TaskStatus.PENDING);
        assertThat(exec.attempt()).isEqualTo(1);
        assertThat(exec.isGlobal()).isFalse();
        assertThat(exec.isFinished()).isFalse();
    }

    @Test
    void shouldQueueTask() {
        TaskExecution exec = TaskExecution.create(UUID.randomUUID(), "task", false, null);
        TaskExecution queued = exec.queue();

        assertThat(queued.status()).isEqualTo(TaskStatus.QUEUED);
    }

    @Test
    void shouldStartTask() {
        TaskExecution exec = TaskExecution.create(UUID.randomUUID(), "task", false, null);
        TaskExecution started = exec.start("worker-1", "worker-1-thread-2");

        assertThat(started.status()).isEqualTo(TaskStatus.RUNNING);
        assertThat(started.workerId()).isEqualTo("worker-1");
        assertThat(started.threadName()).isEqualTo("worker-1-thread-2");
        assertThat(started.startedAt()).isNotNull();
    }

    @Test
    void shouldCompleteTask() {
        TaskExecution exec = TaskExecution.create(UUID.randomUUID(), "task", false, null);
        TaskExecution started = exec.start("worker-1", "thread-1");

        // Small delay to ensure duration > 0
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
        }

        TaskExecution completed = started.complete(Map.of("result", "success"));

        assertThat(completed.status()).isEqualTo(TaskStatus.COMPLETED);
        assertThat(completed.result()).containsEntry("result", "success");
        assertThat(completed.completedAt()).isNotNull();
        assertThat(completed.durationMillis()).isGreaterThan(0);
        assertThat(completed.isFinished()).isTrue();
    }

    @Test
    void shouldFailTask() {
        TaskExecution exec = TaskExecution.create(UUID.randomUUID(), "task", false, null);
        TaskExecution started = exec.start("worker-1", "thread-1");
        TaskExecution failed = started.fail("Error occurred");

        assertThat(failed.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(failed.error()).isEqualTo("Error occurred");
        assertThat(failed.completedAt()).isNotNull();
        assertThat(failed.isFinished()).isTrue();
    }

    @Test
    void shouldSkipTask() {
        TaskExecution exec = TaskExecution.create(UUID.randomUUID(), "task", false, null);
        TaskExecution skipped = exec.skip("Upstream failed");

        assertThat(skipped.status()).isEqualTo(TaskStatus.SKIPPED);
        assertThat(skipped.error()).isEqualTo("Upstream failed");
        assertThat(skipped.isFinished()).isTrue();
    }

    @Test
    void shouldRetryTask() {
        TaskExecution exec = TaskExecution.create(UUID.randomUUID(), "task", false, null);
        TaskExecution started = exec.start("worker-1", "thread-1");
        TaskExecution failed = started.fail("Temporary error");
        TaskExecution retry = failed.retry();

        assertThat(retry.id()).isNotEqualTo(failed.id());
        assertThat(retry.attempt()).isEqualTo(2);
        assertThat(retry.status()).isEqualTo(TaskStatus.PENDING);
        assertThat(retry.error()).isNull();
    }

    @Test
    void shouldNotRetryNonFailedTask() {
        TaskExecution exec = TaskExecution.create(UUID.randomUUID(), "task", false, null);
        TaskExecution completed = exec.complete(Map.of());

        assertThatThrownBy(() -> completed.retry())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot retry");
    }

    @Test
    void shouldRejectNullValues() {
        assertThatThrownBy(() -> TaskExecution.create(null, "task", false, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Graph execution ID cannot be null");

        assertThatThrownBy(() -> TaskExecution.create(UUID.randomUUID(), null, false, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Task name cannot be null");
    }
}