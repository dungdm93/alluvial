package dev.alluvial.sink.iceberg

import dev.alluvial.runtime.SinkConfig
import io.micrometer.core.instrument.MeterRegistry
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.CachingCatalog
import org.apache.iceberg.CatalogProperties.*
import org.apache.iceberg.CatalogUtil
import org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_HADOOP
import org.apache.iceberg.Schema
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.Catalog.TableBuilder
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.SupportsNamespaces
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.exceptions.NoSuchTableException
import org.apache.iceberg.util.PropertyUtil
import org.slf4j.LoggerFactory

class IcebergSink(sinkConfig: SinkConfig, private val registry: MeterRegistry) {
    companion object {
        private val logger = LoggerFactory.getLogger(IcebergSink::class.java)
    }

    private val catalog: Catalog
    private val supportsNamespaces: SupportsNamespaces?

    init {
        val properties = sinkConfig.catalog

        val cacheEnabled = PropertyUtil.propertyAsBoolean(properties, CACHE_ENABLED, CACHE_ENABLED_DEFAULT)
        val cacheExpirationIntervalMs = PropertyUtil.propertyAsLong(
            properties,
            CACHE_EXPIRATION_INTERVAL_MS,
            CACHE_EXPIRATION_INTERVAL_MS_DEFAULT
        )
        val catalogImpl = PropertyUtil.propertyAsString(properties, CATALOG_IMPL, ICEBERG_CATALOG_HADOOP)

        val catalog = CatalogUtil.loadCatalog(catalogImpl, "iceberg", properties, Configuration())
        this.supportsNamespaces = catalog as? SupportsNamespaces
        this.catalog = if (cacheEnabled)
            CachingCatalog.wrap(catalog, cacheExpirationIntervalMs) else
            catalog
    }

    fun getOutlet(name: String, tableId: TableIdentifier): IcebergTableOutlet? {
        return try {
            val table = catalog.loadTable(tableId)

            logger.info("Creating new outlet {}", name)
            IcebergTableOutlet(name, table, registry)
        } catch (e: NoSuchTableException) {
            null
        }
    }

    fun buildTable(tableId: TableIdentifier, schema: Schema, action: (TableBuilder) -> Unit): Table {
        val builder = catalog.buildTable(tableId, schema)
        action(builder)

        ensureNamespace(tableId.namespace())
        return builder.create()
    }

    private fun ensureNamespace(ns: Namespace) {
        if (supportsNamespaces == null) return
        if (supportsNamespaces.namespaceExists(ns)) return

        logger.info("Creating namespace {}", ns)
        supportsNamespaces.createNamespace(ns)
    }

    fun committedOffsets(tableId: TableIdentifier): Map<Int, Long> {
        return try {
            val table = catalog.loadTable(tableId)
            return table.committedOffsets()
        } catch (e: NoSuchTableException) {
            emptyMap()
        }
    }
}
