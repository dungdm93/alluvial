package dev.alluvial.api

import java.io.Closeable

interface Streamlet : Closeable {
    enum class Status {
        CREATED, RUNNING, SUSPENDED, FAILED
    }

    val id: StreamletId
    var status: Status
    fun run()
    fun pause()
    fun resume()
}
