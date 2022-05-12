package dev.alluvial.api

import org.apache.iceberg.Table
import org.apache.iceberg.catalog.TableIdentifier

interface TableCreator {
    fun createTable(topic: String, tableId: TableIdentifier): Table
}
