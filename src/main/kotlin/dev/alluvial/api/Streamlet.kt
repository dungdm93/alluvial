package dev.alluvial.api

import java.io.Closeable

interface Streamlet : Closeable {
    enum class Status {
        CREATED, RUNNING, SUSPENDED, FAILED
    }

    val name: String
    var status: Status
    fun run()
    fun shouldRun(): Boolean
    fun pause()
    fun resume()
}
