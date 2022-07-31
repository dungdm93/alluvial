package dev.alluvial.api

import java.io.Closeable

interface Streamlet : Closeable, Runnable {
    enum class Status {
        CREATED, RUNNING, COMMITTING, SUSPENDED, FAILED
    }

    val name: String
    var status: Status
    fun shouldRun(): Boolean
    fun pause()
    fun resume()
}
