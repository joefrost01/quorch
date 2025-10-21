package org.neuralchilli.quorch.domain;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DagStatisticsTest {

    @Test
    void shouldCreateValidStatistics() {
        DagStatistics stats = new DagStatistics(
                10,  // totalTasks
                2,   // rootTasks
                3,   // leafTasks
                5,   // executionLevels
                4    // maxParallelism
        );

        assertThat(stats.totalTasks()).isEqualTo(10);
        assertThat(stats.rootTasks()).isEqualTo(2);
        assertThat(stats.leafTasks()).isEqualTo(3);
        assertThat(stats.executionLevels()).isEqualTo(5);
        assertThat(stats.maxParallelism()).isEqualTo(4);
        assertThat(stats.depth()).isEqualTo(5);
        assertThat(stats.width()).isEqualTo(4);
    }

    @Test
    void shouldDetectParallelism() {
        DagStatistics parallel = new DagStatistics(5, 1, 1, 3, 2);
        assertThat(parallel.hasParallelism()).isTrue();
        assertThat(parallel.isLinear()).isFalse();

        DagStatistics linear = new DagStatistics(5, 1, 1, 5, 1);
        assertThat(linear.hasParallelism()).isFalse();
        assertThat(linear.isLinear()).isTrue();
    }

    @Test
    void shouldHaveReadableToString() {
        DagStatistics stats = new DagStatistics(10, 2, 3, 5, 4);
        String str = stats.toString();

        assertThat(str).contains("tasks=10");
        assertThat(str).contains("levels=5");
        assertThat(str).contains("max_parallel=4");
        assertThat(str).contains("roots=2");
        assertThat(str).contains("leaves=3");
    }

    @Test
    void shouldRejectNegativeValues() {
        assertThatThrownBy(() -> new DagStatistics(-1, 1, 1, 1, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Total tasks cannot be negative");

        assertThatThrownBy(() -> new DagStatistics(1, -1, 1, 1, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Root tasks cannot be negative");

        assertThatThrownBy(() -> new DagStatistics(1, 1, -1, 1, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Leaf tasks cannot be negative");

        assertThatThrownBy(() -> new DagStatistics(1, 1, 1, -1, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Execution levels cannot be negative");

        assertThatThrownBy(() -> new DagStatistics(1, 1, 1, 1, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Max parallelism cannot be negative");
    }

    @Test
    void shouldAllowZeroValues() {
        // Edge case: empty graph or single task
        DagStatistics empty = new DagStatistics(0, 0, 0, 0, 0);
        assertThat(empty.totalTasks()).isEqualTo(0);

        DagStatistics single = new DagStatistics(1, 1, 1, 1, 1);
        assertThat(single.totalTasks()).isEqualTo(1);
        assertThat(single.isLinear()).isTrue();
    }

    @Test
    void shouldProvideDepthAndWidthAliases() {
        DagStatistics stats = new DagStatistics(10, 2, 3, 5, 4);

        // depth() is alias for executionLevels
        assertThat(stats.depth()).isEqualTo(stats.executionLevels());
        assertThat(stats.depth()).isEqualTo(5);

        // width() is alias for maxParallelism
        assertThat(stats.width()).isEqualTo(stats.maxParallelism());
        assertThat(stats.width()).isEqualTo(4);
    }

    @Test
    void shouldHandleEdgeCases() {
        // Highly parallel graph (wide but shallow)
        DagStatistics wide = new DagStatistics(100, 1, 1, 2, 50);
        assertThat(wide.hasParallelism()).isTrue();
        assertThat(wide.width()).isGreaterThan(wide.depth());

        // Deeply sequential graph (narrow but deep)
        DagStatistics deep = new DagStatistics(100, 1, 1, 100, 1);
        assertThat(deep.isLinear()).isTrue();
        assertThat(deep.depth()).isGreaterThan(deep.width());
    }
}