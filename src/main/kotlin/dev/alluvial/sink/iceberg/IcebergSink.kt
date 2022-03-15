package dev.alluvial.sink.iceberg

import dev.alluvial.api.StreamletId
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.CachingCatalog
import org.apache.iceberg.CatalogProperties.*
import org.apache.iceberg.CatalogUtil
import org.apache.iceberg.TableProperties
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.exceptions.NoSuchTableException
import org.slf4j.LoggerFactory
import org.apache.iceberg.Schema as IcebergSchema

class IcebergSink(config: Map<String, Any>) {
    companion object {
        private val logger = LoggerFactory.getLogger(IcebergSink::class.java)
        private val NAMED_CATALOG = mapOf(
            "hadoop" to "org.apache.iceberg.hadoop.HadoopCatalog",
            "hive" to "org.apache.iceberg.hive.HiveCatalog",
            "jdbc" to "org.apache.iceberg.jdbc.JdbcCatalog",
            "glue" to "org.apache.iceberg.aws.glue.GlueCatalog",
            "dynamodb" to "org.apache.iceberg.aws.dynamodb.DynamoDbCatalog",
            "nessie" to "org.apache.iceberg.nessie.NessieCatalog",
        )

        const val LOCATION_BASE_PROP = "alluvial.sink.iceberg.location-base"
    }

    private val properties: Map<String, String>
    private val catalog: Catalog
    private val cacheEnabled: Boolean
    private val locationBase: String?

    init {
        this.properties = config.mapValues { (_, v) -> v.toString() }

        this.cacheEnabled = properties.getOrDefault(CACHE_ENABLED, "true").toBoolean()
        val cacheExpirationInterval = properties.getOrDefault(
            CACHE_EXPIRATION_INTERVAL_MS,
            CACHE_EXPIRATION_INTERVAL_MS_DEFAULT.toString()
        ).toLong()
        val catalogImpl = NAMED_CATALOG.getOrDefault(properties["catalog-type"], properties[CATALOG_IMPL])
        requireNotNull(catalogImpl) { "catalogImpl must not be null" }

        val catalog = CatalogUtil.loadCatalog(catalogImpl, "iceberg", properties, Configuration())
        this.catalog = if (cacheEnabled)
            CachingCatalog.wrap(catalog, cacheExpirationInterval) else
            catalog

        this.locationBase = properties[LOCATION_BASE_PROP]?.removeSuffix("/")
    }

    fun getOutlet(id: StreamletId): IcebergTableOutlet? {
        return try {
            val identifier = TableIdentifier.of(id.schema, id.table)
            val table = catalog.loadTable(identifier)
            IcebergTableOutlet(id, table, properties)
        } catch (e: NoSuchTableException) {
            null
        }
    }

    fun createOutlet(id: StreamletId, schema: IcebergSchema): IcebergTableOutlet {
        val tableId = TableIdentifier.of(id.schema, id.table)
        val tableBuilder = catalog.buildTable(tableId, schema)
            .withProperty(TableProperties.FORMAT_VERSION, "2")

        // TODO other table attributes
        if (locationBase != null)
            tableBuilder.withLocation("$locationBase/${id.schema}/${id.table}")

        val table = tableBuilder.create()
        return IcebergTableOutlet(id, table, properties)
    }

    fun committedPositions(id: StreamletId): Map<Int, Long>? {
        val outlet = getOutlet(id)
        return outlet?.committedPositions()
    }

    fun committedTimestamp(id: StreamletId): Long? {
        val outlet = getOutlet(id)
        return outlet?.committedTimestamp()
    }
}
