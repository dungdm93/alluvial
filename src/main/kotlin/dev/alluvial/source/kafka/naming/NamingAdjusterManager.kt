package dev.alluvial.source.kafka.naming

import org.apache.iceberg.catalog.TableIdentifier

/**
 * @see org.apache.iceberg.catalog.TableIdentifier
 * @see io.debezium.relational.TableId
 * @see org.apache.spark.sql.connector.catalog.Identifier
 * @see org.apache.flink.table.catalog.ObjectPath
 * @see org.apache.flink.table.catalog.ObjectIdentifier
 */
class NamingAdjusterManager(naConfigs: List<Map<String, Any>>) {
    companion object {
        private fun loadNamingAdjuster(className: String, config: Map<String, Any>): NamingAdjuster {
            val klass = Class.forName(className)
            val na = klass.getDeclaredConstructor().newInstance() as NamingAdjuster

            na.configure(config)
            return na
        }
    }

    private val namingAdjusters = naConfigs.map { config ->
        val className = config["class"] as String
        loadNamingAdjuster(className, config)
    }

    fun adjustNamespace(namespace: List<String>): List<String> {
        var ns = namespace
        namingAdjusters.forEach { na ->
            ns = na.adjustNamespace(ns)
        }
        return ns
    }

    fun adjustTable(namespace: List<String>, table: String): String {
        var tbl = table
        namingAdjusters.forEach { na ->
            tbl = na.adjustTable(namespace, tbl)
        }
        return tbl
    }

    fun adjustColumn(tableId: TableIdentifier, column: String): String {
        var col = column
        namingAdjusters.forEach { na ->
            col = na.adjustColumn(tableId, column)
        }
        return col
    }
}
