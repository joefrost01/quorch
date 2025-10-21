package org.neuralchilli.quorch.domain;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkerTest {

    @Test
    void shouldCreateNewWorker() {
        Worker worker = Worker.create("worker-1", 4);

        assertThat(worker.id()).isEqualTo("worker-1");
        assertThat(worker.totalThreads()).isEqualTo(4);
        assertThat(worker.activeThreads()).isEqualTo(0);
        assertThat(worker.status()).isEqualTo(WorkerStatus.ACTIVE);
        assertThat(worker.lastHeartbeat()).isNotNull();
        assertThat(worker.startedAt()).isNotNull();
    }

    @Test
    void shouldUpdateHeartbeat() {
        Worker worker = Worker.create("worker-1", 4);

        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
        }

        Worker updated = worker.heartbeat();

        assertThat(updated.lastHeartbeat()).isAfter(worker.lastHeartbeat());
    }

    @Test
    void shouldUpdateActiveThreads() {
        Worker worker = Worker.create("worker-1", 4);
        Worker updated = worker.withActiveThreads(2);

        assertThat(updated.activeThreads()).isEqualTo(2);
        assertThat(updated.availableThreads()).isEqualTo(2);
    }

    @Test
    void shouldDrainWorker() {
        Worker worker = Worker.create("worker-1", 4);
        Worker draining = worker.drain();

        assertThat(draining.status()).isEqualTo(WorkerStatus.DRAINING);
        assertThat(draining.hasCapacity()).isFalse();
    }

    @Test
    void shouldCheckHealth() {
        Worker worker = Worker.create("worker-1", 4);

        // Fresh worker should be healthy
        assertThat(worker.isHealthy(Duration.ofSeconds(60))).isTrue();

        // Mark as dead
        Worker dead = worker.withStatus(WorkerStatus.DEAD);
        assertThat(dead.isHealthy(Duration.ofSeconds(60))).isFalse();
    }

    @Test
    void shouldCalculateAvailableThreads() {
        Worker worker = Worker.create("worker-1", 10);
        Worker busy = worker.withActiveThreads(7);

        assertThat(busy.availableThreads()).isEqualTo(3);
        assertThat(busy.hasCapacity()).isTrue();
    }

    @Test
    void shouldNotHaveCapacityWhenFull() {
        Worker worker = Worker.create("worker-1", 4);
        Worker full = worker.withActiveThreads(4);

        assertThat(full.hasCapacity()).isFalse();
    }

    @Test
    void shouldNotHaveCapacityWhenDraining() {
        Worker worker = Worker.create("worker-1", 4);
        Worker draining = worker.drain();

        assertThat(draining.hasCapacity()).isFalse();
    }

    @Test
    void shouldRejectInvalidValues() {
        assertThatThrownBy(() -> Worker.create(null, 4))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Worker ID cannot be null");

        assertThatThrownBy(() -> Worker.create("worker", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Total threads must be > 0");

        Worker worker = Worker.create("worker-1", 4);

        assertThatThrownBy(() -> worker.withActiveThreads(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Active threads cannot be negative");

        assertThatThrownBy(() -> worker.withActiveThreads(5))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Active threads cannot exceed total threads");
    }
}