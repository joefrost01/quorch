package org.neuralchilli.quorch.monitoring;

import jakarta.enterprise.context.ApplicationScoped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Performance monitoring utility to track optimization gains.
 *
 * Tracks metrics for:
 * - DAG cache hit rates
 * - Optimistic locking retry rates
 * - Batch operation efficiency
 * - Task throughput
 *
 * Use this to verify performance improvements are working as expected.
 */
@ApplicationScoped
public class PerformanceMonitor {

    private static final Logger log = LoggerFactory.getLogger(PerformanceMonitor.class);

    // DAG cache metrics
    private final LongAdder dagCacheHits = new LongAdder();
    private final LongAdder dagCacheMisses = new LongAdder();

    // Optimistic locking metrics
    private final LongAdder optimisticLockSuccesses = new LongAdder();
    private final LongAdder optimisticLockRetries = new LongAdder();
    private final LongAdder optimisticLockFailures = new LongAdder();

    // Batch operation metrics
    private final LongAdder batchOperations = new LongAdder();
    private final LongAdder batchedItems = new LongAdder();
    private final LongAdder networkCallsSaved = new LongAdder();

    // Task execution metrics
    private final LongAdder tasksScheduled = new LongAdder();
    private final LongAdder tasksCompleted = new LongAdder();
    private final LongAdder tasksFailed = new LongAdder();

    // Timing metrics
    private final Map<String, TimingStats> timingStats = new ConcurrentHashMap<>();

    // Worker pool metrics
    private final LongAdder workerWakeups = new LongAdder();
    private final LongAdder workerTimeouts = new LongAdder();

    /**
     * Record a DAG cache hit.
     */
    public void recordDagCacheHit() {
        dagCacheHits.increment();
    }

    /**
     * Record a DAG cache miss (new DAG built).
     */
    public void recordDagCacheMiss() {
        dagCacheMisses.increment();
    }

    /**
     * Get DAG cache hit rate.
     */
    public double getDagCacheHitRate() {
        long hits = dagCacheHits.sum();
        long misses = dagCacheMisses.sum();
        long total = hits + misses;
        return total > 0 ? (hits * 100.0) / total : 0.0;
    }

    /**
     * Record a successful optimistic lock operation.
     */
    public void recordOptimisticLockSuccess() {
        optimisticLockSuccesses.increment();
    }

    /**
     * Record an optimistic lock retry.
     */
    public void recordOptimisticLockRetry() {
        optimisticLockRetries.increment();
    }

    /**
     * Record an optimistic lock failure (exceeded max retries).
     */
    public void recordOptimisticLockFailure() {
        optimisticLockFailures.increment();
    }

    /**
     * Get optimistic lock success rate.
     */
    public double getOptimisticLockSuccessRate() {
        long successes = optimisticLockSuccesses.sum();
        long failures = optimisticLockFailures.sum();
        long total = successes + failures;
        return total > 0 ? (successes * 100.0) / total : 0.0;
    }

    /**
     * Get average retries per optimistic lock operation.
     */
    public double getAverageOptimisticLockRetries() {
        long retries = optimisticLockRetries.sum();
        long operations = optimisticLockSuccesses.sum() + optimisticLockFailures.sum();
        return operations > 0 ? (double) retries / operations : 0.0;
    }

    /**
     * Record a batch operation.
     *
     * @param itemCount Number of items in the batch
     * @param callsSaved Number of network calls saved by batching
     */
    public void recordBatchOperation(int itemCount, int callsSaved) {
        batchOperations.increment();
        batchedItems.add(itemCount);
        networkCallsSaved.add(callsSaved);
    }

    /**
     * Get average batch size.
     */
    public double getAverageBatchSize() {
        long operations = batchOperations.sum();
        long items = batchedItems.sum();
        return operations > 0 ? (double) items / operations : 0.0;
    }

    /**
     * Get total network calls saved by batching.
     */
    public long getNetworkCallsSaved() {
        return networkCallsSaved.sum();
    }

    /**
     * Record a task scheduled.
     */
    public void recordTaskScheduled() {
        tasksScheduled.increment();
    }

    /**
     * Record a task completed.
     */
    public void recordTaskCompleted() {
        tasksCompleted.increment();
    }

    /**
     * Record a task failed.
     */
    public void recordTaskFailed() {
        tasksFailed.increment();
    }

    /**
     * Get task success rate.
     */
    public double getTaskSuccessRate() {
        long completed = tasksCompleted.sum();
        long failed = tasksFailed.sum();
        long total = completed + failed;
        return total > 0 ? (completed * 100.0) / total : 0.0;
    }

    /**
     * Get current task throughput (tasks/second over last period).
     */
    public double getTaskThroughput(Duration period) {
        // This would need a rolling window implementation
        // For now, return completed tasks / period
        long completed = tasksCompleted.sum();
        return completed / period.getSeconds();
    }

    /**
     * Record worker wakeup (event-driven).
     */
    public void recordWorkerWakeup() {
        workerWakeups.increment();
    }

    /**
     * Record worker timeout (fallback polling).
     */
    public void recordWorkerTimeout() {
        workerTimeouts.increment();
    }

    /**
     * Get worker event-driven efficiency (% of wakeups vs timeouts).
     */
    public double getWorkerEventEfficiency() {
        long wakeups = workerWakeups.sum();
        long timeouts = workerTimeouts.sum();
        long total = wakeups + timeouts;
        return total > 0 ? (wakeups * 100.0) / total : 0.0;
    }

    /**
     * Start timing an operation.
     *
     * @param operation Operation name
     * @return Timer handle to stop timing
     */
    public Timer startTimer(String operation) {
        return new Timer(operation, Instant.now());
    }

    /**
     * Timer handle for operation timing.
     */
    public class Timer {
        private final String operation;
        private final Instant start;

        private Timer(String operation, Instant start) {
            this.operation = operation;
            this.start = start;
        }

        /**
         * Stop timing and record duration.
         */
        public void stop() {
            Duration duration = Duration.between(start, Instant.now());
            recordTiming(operation, duration);
        }
    }

    /**
     * Record timing for an operation.
     */
    private void recordTiming(String operation, Duration duration) {
        timingStats.compute(operation, (key, stats) -> {
            if (stats == null) {
                stats = new TimingStats();
            }
            stats.record(duration);
            return stats;
        });
    }

    /**
     * Get timing statistics for an operation.
     */
    public TimingStats getTimingStats(String operation) {
        return timingStats.getOrDefault(operation, new TimingStats());
    }

    /**
     * Statistics for operation timing.
     */
    public static class TimingStats {
        private final LongAdder count = new LongAdder();
        private final LongAdder totalNanos = new LongAdder();
        private final AtomicLong minNanos = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong maxNanos = new AtomicLong(0);

        void record(Duration duration) {
            long nanos = duration.toNanos();
            count.increment();
            totalNanos.add(nanos);

            // Update min
            minNanos.updateAndGet(current -> Math.min(current, nanos));

            // Update max
            maxNanos.updateAndGet(current -> Math.max(current, nanos));
        }

        public long getCount() {
            return count.sum();
        }

        public Duration getAverage() {
            long total = totalNanos.sum();
            long cnt = count.sum();
            return cnt > 0 ? Duration.ofNanos(total / cnt) : Duration.ZERO;
        }

        public Duration getMin() {
            long min = minNanos.get();
            return min < Long.MAX_VALUE ? Duration.ofNanos(min) : Duration.ZERO;
        }

        public Duration getMax() {
            return Duration.ofNanos(maxNanos.get());
        }

        @Override
        public String toString() {
            return String.format(
                    "TimingStats[count=%d, avg=%dms, min=%dms, max=%dms]",
                    getCount(),
                    getAverage().toMillis(),
                    getMin().toMillis(),
                    getMax().toMillis()
            );
        }
    }

    /**
     * Get comprehensive performance report.
     */
    public PerformanceReport getReport() {
        return new PerformanceReport(
                getDagCacheHitRate(),
                dagCacheHits.sum(),
                dagCacheMisses.sum(),
                getOptimisticLockSuccessRate(),
                getAverageOptimisticLockRetries(),
                optimisticLockSuccesses.sum(),
                optimisticLockFailures.sum(),
                getAverageBatchSize(),
                getNetworkCallsSaved(),
                batchOperations.sum(),
                getTaskSuccessRate(),
                tasksScheduled.sum(),
                tasksCompleted.sum(),
                tasksFailed.sum(),
                getWorkerEventEfficiency(),
                workerWakeups.sum(),
                workerTimeouts.sum()
        );
    }

    /**
     * Performance report snapshot.
     */
    public record PerformanceReport(
            double dagCacheHitRate,
            long dagCacheHits,
            long dagCacheMisses,
            double optimisticLockSuccessRate,
            double avgOptimisticLockRetries,
            long optimisticLockSuccesses,
            long optimisticLockFailures,
            double avgBatchSize,
            long networkCallsSaved,
            long batchOperations,
            double taskSuccessRate,
            long tasksScheduled,
            long tasksCompleted,
            long tasksFailed,
            double workerEventEfficiency,
            long workerWakeups,
            long workerTimeouts
    ) {
        @Override
        public String toString() {
            return String.format("""
                Performance Report:
                ==================
                DAG Cache:
                  Hit Rate: %.1f%% (%d hits, %d misses)
                  
                Optimistic Locking:
                  Success Rate: %.1f%% (%d successes, %d failures)
                  Avg Retries: %.2f
                  
                Batch Operations:
                  Operations: %d
                  Avg Batch Size: %.1f items
                  Network Calls Saved: %d
                  
                Task Execution:
                  Success Rate: %.1f%%
                  Scheduled: %d, Completed: %d, Failed: %d
                  
                Worker Pool:
                  Event Efficiency: %.1f%% (%d wakeups, %d timeouts)
                """,
                    dagCacheHitRate, dagCacheHits, dagCacheMisses,
                    optimisticLockSuccessRate, optimisticLockSuccesses, optimisticLockFailures,
                    avgOptimisticLockRetries,
                    batchOperations, avgBatchSize, networkCallsSaved,
                    taskSuccessRate, tasksScheduled, tasksCompleted, tasksFailed,
                    workerEventEfficiency, workerWakeups, workerTimeouts
            );
        }
    }

    /**
     * Reset all metrics (useful for testing).
     */
    public void reset() {
        dagCacheHits.reset();
        dagCacheMisses.reset();
        optimisticLockSuccesses.reset();
        optimisticLockRetries.reset();
        optimisticLockFailures.reset();
        batchOperations.reset();
        batchedItems.reset();
        networkCallsSaved.reset();
        tasksScheduled.reset();
        tasksCompleted.reset();
        tasksFailed.reset();
        workerWakeups.reset();
        workerTimeouts.reset();
        timingStats.clear();
        log.info("Performance metrics reset");
    }

    /**
     * Log current performance report.
     */
    public void logReport() {
        log.info("\n{}", getReport());
    }
}