package dev.alluvial.utils

interface Time {
    fun millis(): Long
    fun nanos(): Long
}

object SystemTime : Time {
    override fun millis(): Long {
        return System.currentTimeMillis()
    }

    override fun nanos(): Long {
        return System.nanoTime()
    }
}
