package dev.alluvial.api

data class StreamletId(
    val schema: String,
    val table: String,
) {
    override fun toString() = "$schema.$table"
}
