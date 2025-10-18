package org.neuralchilli.quorch.domain;

import org.junit.jupiter.api.Test;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class GraphExecutionTest {

    @Test
    void shouldCreateNewExecution() {
        GraphExecution exec = GraphExecution.create(
                "test-graph",
                Map.of("param1", "value1"),
                "manual"
        );

        assertThat(exec.id()).isNotNull();
        assertThat(exec.graphName()).isEqualTo("test-graph");
        assertThat(exec.params()).containsEntry("param1", "value1");
        assertThat(exec.status()).isEqualTo(GraphStatus.RUNNING);
        assertThat(exec.triggeredBy()).isEqualTo("manual");
        assertThat(exec.startedAt()).isNotNull();
        assertThat(exec.completedAt()).isNull();
        assertThat(exec.error()).isNull();
    }

    @Test
    void shouldUpdateStatus() {
        GraphExecution exec = GraphExecution.create("test", Map.of(), "test");
        GraphExecution updated = exec.withStatus(GraphStatus.PAUSED);

        assertThat(updated.status()).isEqualTo(GraphStatus.PAUSED);
        assertThat(updated.id()).isEqualTo(exec.id());
    }

    @Test
    void shouldCompleteExecution() {
        GraphExecution exec = GraphExecution.create("test", Map.of(), "test");
        GraphExecution completed = exec.complete();

        assertThat(completed.status()).isEqualTo(GraphStatus.COMPLETED);
        assertThat(completed.completedAt()).isNotNull();
        assertThat(completed.isFinished()).isTrue();
    }

    @Test
    void shouldFailExecution() {
        GraphExecution exec = GraphExecution.create("test", Map.of(), "test");
        GraphExecution failed = exec.fail("Something went wrong");

        assertThat(failed.status()).isEqualTo(GraphStatus.FAILED);
        assertThat(failed.error()).isEqualTo("Something went wrong");
        assertThat(failed.completedAt()).isNotNull();
        assertThat(failed.isFinished()).isTrue();
    }

    @Test
    void shouldStallExecution() {
        GraphExecution exec = GraphExecution.create("test", Map.of(), "test");
        GraphExecution stalled = exec.stall();

        assertThat(stalled.status()).isEqualTo(GraphStatus.STALLED);
        assertThat(stalled.completedAt()).isNotNull();
        assertThat(stalled.isFinished()).isTrue();
    }

    @Test
    void shouldPauseAndResumeExecution() {
        GraphExecution exec = GraphExecution.create("test", Map.of(), "test");
        GraphExecution paused = exec.pause();

        assertThat(paused.status()).isEqualTo(GraphStatus.PAUSED);

        GraphExecution resumed = paused.resume();
        assertThat(resumed.status()).isEqualTo(GraphStatus.RUNNING);
        assertThat(resumed.completedAt()).isNull();
    }

    @Test
    void shouldNotResumeNonPausedExecution() {
        GraphExecution exec = GraphExecution.create("test", Map.of(), "test");

        assertThatThrownBy(() -> exec.resume())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot resume");
    }

    @Test
    void shouldRejectNullGraphName() {
        assertThatThrownBy(() -> GraphExecution.create(null, Map.of(), "test"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Graph name cannot be null");
    }
}