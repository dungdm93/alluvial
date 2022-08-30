package dev.alluvial.utils

import org.slf4j.LoggerFactory
import java.util.Queue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.atomic.AtomicReference

/**
 * A wrapper of [ExecutorService] that all tasks in the same group
 * must be run in order - it's similar to the swim lane.
 */
class LanePoolRunner<K, I, O>(
    private val executor: ExecutorService,
    private val task: Task<I, O>,
) {
    companion object {
        private val logger = LoggerFactory.getLogger(LanePoolRunner::class.java)
    }

    private val listeners = mutableListOf<Callback<I, O>>()
    private val laneMap = ConcurrentHashMap<K, Lane>()

    fun addListener(listener: Callback<I, O>) {
        listeners.add(listener)
    }

    fun enqueue(key: K, input: I) {
        val lane = laneMap.computeIfAbsent(key) {
            logger.info("Creating new lane for {}", key)
            Lane()
        }
        lane.enqueue(input)
    }

    fun runningItem(key: K): I? {
        return laneMap[key]?.runningItem()
    }

    fun isEmpty(key: K): Boolean {
        val lane = laneMap[key] ?: return true
        return lane.isEmpty()
    }

    fun queueSize(key: K): Int {
        val lane = laneMap[key] ?: return 0
        return lane.size()
    }

    inner class Lane : Callback<I, O> {
        private var currentItem = AtomicReference<I?>(null)
        private val queue: Queue<I> = ConcurrentLinkedQueue()

        fun runningItem(): I? {
            return currentItem.get()
        }

        fun isEmpty(): Boolean {
            return runningItem() == null && queue.isEmpty()
        }

        fun size(): Int {
            return queue.size
        }

        fun enqueue(input: I) {
            queue.add(input)
            runNext()
        }

        private fun runNext() {
            if (isEmpty()) return

            if (!currentItem.compareAndSet(null, queue.element()))
                return logger.debug("Another task is running.")
            else
                queue.remove()

            val item = currentItem.get()!!
            val callbackTask = createNextTask()
            executor.submit {
                callbackTask.run(item)
            }
        }

        private fun createNextTask(): Task<I, O> {
            val callbackTask = CallbackTask(task)
            listeners.forEach(callbackTask::addCallback)
            callbackTask.addCallback(this)
            return callbackTask
        }

        override fun onSuccess(input: I, result: O) {
            if (currentItem.compareAndSet(input, null)) {
                return runNext()
            }
            logger.error("Something went wrong, currentItem has changed.")
        }

        override fun onFailure(input: I, throwable: Throwable) {
            currentItem.compareAndSet(input, null)
        }
    }
}
