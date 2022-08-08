package dev.alluvial.utils

import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit
import kotlin.math.max

/**
 * @see kotlinx.coroutines.scheduling.DefaultScheduler.IO
 */
fun recommendedPoolSize(): Int {
    val size = Runtime.getRuntime().availableProcessors() * 2 + 1
    return max(64, size)
}

/**
 * [ExecutorService docs](https://docs.oracle.com/javase/9/docs/api/java/util/concurrent/ExecutorService.html)
 * @see com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination
 */
fun ExecutorService.shutdownAndAwaitTermination(timeout: Long, unit: TimeUnit): Boolean {
    val halfTimeoutNanos = unit.toNanos(timeout) / 2L

    // Disable new tasks from being submitted
    this.shutdown()
    try {
        // Wait for half the duration of the timeout for existing tasks to terminate
        if (!this.awaitTermination(halfTimeoutNanos, TimeUnit.NANOSECONDS)) {
            // Cancel currently executing tasks
            this.shutdownNow()
            // Wait the other half of the timeout for tasks to respond to being cancelled
            this.awaitTermination(halfTimeoutNanos, TimeUnit.NANOSECONDS)
        }
    } catch (ie: InterruptedException) {
        // Preserve interrupt status
        Thread.currentThread().interrupt()
        // (Re-)Cancel if current thread also interrupted
        this.shutdownNow()
    }

    return this.isTerminated
}
