package org.neuralchilli.quorch.domain;

import org.junit.jupiter.api.Test;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

class GlobalTaskExecutionTest {

    @Test
    void shouldCreateNewGlobalExecution() {
        UUID graphExecId = UUID.randomUUID();
        GlobalTaskExecution exec = GlobalTaskExecution.create(
                "load-data",
                "load_2025-10-17_us",
                Map.of("date", "2025-10-17", "region", "us"),
                graphExecId
        );

        assertThat(exec.id()).isNotNull();
        assertThat(exec.taskName()).isEqualTo("load-data");
        assertThat(exec.resolvedKey()).isEqualTo("load_2025-10-17_us");
        assertThat(exec.params()).containsEntry("date", "2025-10-17");
        assertThat(exec.status()).isEqualTo(TaskStatus.PENDING);
        assertThat(exec.linkedGraphExecutions()).containsExactly(graphExecId);
        assertThat(exec.linkedGraphCount()).isEqualTo(1);
    }

    @Test
    void shouldLinkAdditionalGraphs() {
        UUID graph1 = UUID.randomUUID();
        UUID graph2 = UUID.randomUUID();

        GlobalTaskExecution exec = GlobalTaskExecution.create(
                "task", "key", Map.of(), graph1
        );

        GlobalTaskExecution linked = exec.linkGraph(graph2);

        assertThat(linked.linkedGraphExecutions()).containsExactlyInAnyOrder(graph1, graph2);
        assertThat(linked.linkedGraphCount()).isEqualTo(2);
    }

    @Test
    void shouldQueueGlobalTask() {
        GlobalTaskExecution exec = GlobalTaskExecution.create(
                "task", "key", Map.of(), UUID.randomUUID()
        );
        GlobalTaskExecution queued = exec.queue();

        assertThat(queued.status()).isEqualTo(TaskStatus.QUEUED);
    }

    @Test
    void shouldStartGlobalTask() {
        GlobalTaskExecution exec = GlobalTaskExecution.create(
                "task", "key", Map.of(), UUID.randomUUID()
        );
        GlobalTaskExecution started = exec.start("worker-1", "thread-1");

        assertThat(started.status()).isEqualTo(TaskStatus.RUNNING);
        assertThat(started.workerId()).isEqualTo("worker-1");
        assertThat(started.threadName()).isEqualTo("thread-1");
        assertThat(started.startedAt()).isNotNull();
    }

    @Test
    void shouldCompleteGlobalTask() {
        GlobalTaskExecution exec = GlobalTaskExecution.create(
                "task", "key", Map.of(), UUID.randomUUID()
        );
        GlobalTaskExecution started = exec.start("worker-1", "thread-1");
        GlobalTaskExecution completed = started.complete(Map.of("rows", 1000));

        assertThat(completed.status()).isEqualTo(TaskStatus.COMPLETED);
        assertThat(completed.result()).containsEntry("rows", 1000);
        assertThat(completed.completedAt()).isNotNull();
        assertThat(completed.isFinished()).isTrue();
    }

    @Test
    void shouldFailGlobalTask() {
        GlobalTaskExecution exec = GlobalTaskExecution.create(
                "task", "key", Map.of(), UUID.randomUUID()
        );
        GlobalTaskExecution started = exec.start("worker-1", "thread-1");
        GlobalTaskExecution failed = started.fail("Connection timeout");

        assertThat(failed.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(failed.error()).isEqualTo("Connection timeout");
        assertThat(failed.isFinished()).isTrue();
    }

    @Test
    void shouldGetExecutionKey() {
        GlobalTaskExecution exec = GlobalTaskExecution.create(
                "load-data", "load_2025-10-17", Map.of(), UUID.randomUUID()
        );
        TaskExecutionKey key = exec.getKey();

        assertThat(key.taskName()).isEqualTo("load-data");
        assertThat(key.resolvedKey()).isEqualTo("load_2025-10-17");
    }

    @Test
    void shouldRejectNullValues() {
        assertThatThrownBy(() -> GlobalTaskExecution.create(
                null, "key", Map.of(), UUID.randomUUID()
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Task name cannot be null");

        assertThatThrownBy(() -> GlobalTaskExecution.create(
                "task", null, Map.of(), UUID.randomUUID()
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Resolved key cannot be null");
    }
}