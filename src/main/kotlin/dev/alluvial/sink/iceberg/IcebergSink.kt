package dev.alluvial.sink.iceberg

import dev.alluvial.api.StreamletId
import dev.alluvial.runtime.SinkConfig
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.CachingCatalog
import org.apache.iceberg.CatalogProperties.*
import org.apache.iceberg.CatalogUtil
import org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_HADOOP
import org.apache.iceberg.Schema
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.Catalog.TableBuilder
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.SupportsNamespaces
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.exceptions.NoSuchTableException
import org.apache.iceberg.util.PropertyUtil
import org.slf4j.LoggerFactory

@Suppress("MemberVisibilityCanBePrivate")
class IcebergSink(sinkConfig: SinkConfig) {
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

    fun getOutlet(id: StreamletId): IcebergTableOutlet? {
        return try {
            val tableId = tableIdentifierOf(id)
            val table = catalog.loadTable(tableId)
            IcebergTableOutlet(id, table)
        } catch (e: NoSuchTableException) {
            null
        }
    }

    fun newTableBuilder(tableId: TableIdentifier, schema: Schema): TableBuilder {
        return catalog.buildTable(tableId, schema)
    }

    fun ensureNamespace(ns: Namespace) {
        if (supportsNamespaces == null) return
        if (supportsNamespaces.namespaceExists(ns)) return
        supportsNamespaces.createNamespace(ns)
    }

    fun committedOffsets(id: StreamletId): Map<Int, Long>? {
        val outlet = getOutlet(id)
        return outlet?.committedOffsets()
    }

    fun idOf(tableId: TableIdentifier): StreamletId {
        val ns = tableId.namespace().levels()
        assert(ns.size == 1) { "table' schema must be in ONE level, got ${ns.size} of $ns" }
        return StreamletId(ns.first(), tableId.name())
    }

    fun tableIdentifierOf(id: StreamletId): TableIdentifier {
        return TableIdentifier.of(id.schema, id.table)
    }
}
