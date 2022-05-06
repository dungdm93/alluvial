package dev.alluvial.source.kafka.naming

import org.apache.iceberg.catalog.TableIdentifier

/**
 * `schema.name.adjustment.mode`
 * `sanitize.field.names`
 */
interface NamingAdjuster {
    val name: String

    fun configure(config: Map<String, Any>)

    fun adjustNamespace(namespace: List<String>): List<String> = namespace
    fun adjustTable(namespace: List<String>, table: String): String = table
    fun adjustColumn(tableId: TableIdentifier, column: String): String = column
}
