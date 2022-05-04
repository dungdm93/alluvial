package dev.alluvial.api

import org.apache.iceberg.Table

interface TableCreator {
    fun createTable(id: StreamletId): Table
}
