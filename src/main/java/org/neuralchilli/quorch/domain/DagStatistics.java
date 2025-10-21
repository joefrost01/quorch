package org.neuralchilli.quorch.domain;

import javax.annotation.Nonnull;

/**
 * Statistics about a workflow DAG structure.
 * Useful for monitoring and optimization.
 */
public record DagStatistics(
        int totalTasks,
        int rootTasks,
        int leafTasks,
        int executionLevels,
        int maxParallelism
) {
    public DagStatistics {
        if (totalTasks < 0) {
            throw new IllegalArgumentException("Total tasks cannot be negative");
        }
        if (rootTasks < 0) {
            throw new IllegalArgumentException("Root tasks cannot be negative");
        }
        if (leafTasks < 0) {
            throw new IllegalArgumentException("Leaf tasks cannot be negative");
        }
        if (executionLevels < 0) {
            throw new IllegalArgumentException("Execution levels cannot be negative");
        }
        if (maxParallelism < 0) {
            throw new IllegalArgumentException("Max parallelism cannot be negative");
        }
    }

    /**
     * Check if the graph has any parallelism opportunity
     */
    public boolean hasParallelism() {
        return maxParallelism > 1;
    }

    /**
     * Check if the graph is linear (no parallelism)
     */
    public boolean isLinear() {
        return maxParallelism == 1;
    }

    /**
     * Get the depth of the DAG (number of sequential execution levels)
     */
    public int depth() {
        return executionLevels;
    }

    /**
     * Get the width of the DAG (maximum parallelism)
     */
    public int width() {
        return maxParallelism;
    }

    @Nonnull
    @Override
    public String toString() {
        return String.format(
                "DagStatistics[tasks=%d, levels=%d, max_parallel=%d, roots=%d, leaves=%d]",
                totalTasks, executionLevels, maxParallelism, rootTasks, leafTasks
        );
    }
}