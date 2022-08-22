package dev.alluvial.runtime

import com.google.common.collect.Maps
import com.google.common.collect.Multimaps
import com.google.common.collect.SetMultimap
import com.google.common.collect.Sets
import dev.alluvial.metrics.MetricsService
import dev.alluvial.utils.Callback
import dev.alluvial.utils.CompactionGroup
import dev.alluvial.utils.CompactionPoints
import dev.alluvial.utils.LanePoolRunner
import dev.alluvial.utils.recommendedPoolSize
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.binder.iceberg.IcebergTableMetrics
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.CachingCatalog
import org.apache.iceberg.CatalogProperties
import org.apache.iceberg.CatalogUtil
import org.apache.iceberg.CompactSnapshots
import org.apache.iceberg.Snapshot
import org.apache.iceberg.TableOperations
import org.apache.iceberg.ancestorsOf
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.util.PropertyUtil
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.ZonedDateTime
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.Executors
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.TimeUnit.MILLISECONDS

class TableManager : Runnable {
    companion object {
        private val logger = LoggerFactory.getLogger(TableManager::class.java)
        private val executor = Executors.newScheduledThreadPool(recommendedPoolSize())
        private val registry = Metrics.globalRegistry

        fun loadCatalog(properties: Map<String, String>): Catalog {
            val cacheEnabled = PropertyUtil.propertyAsBoolean(
                properties,
                CatalogProperties.CACHE_ENABLED,
                CatalogProperties.CACHE_ENABLED_DEFAULT
            )
            val cacheExpirationIntervalMs = PropertyUtil.propertyAsLong(
                properties,
                CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS,
                CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS_DEFAULT
            )
            val catalogImpl = PropertyUtil.propertyAsString(
                properties,
                CatalogProperties.CATALOG_IMPL,
                CatalogUtil.ICEBERG_CATALOG_HADOOP
            )

            val catalog = CatalogUtil.loadCatalog(catalogImpl, "iceberg", properties, Configuration())
            return if (cacheEnabled)
                CachingCatalog.wrap(catalog, cacheExpirationIntervalMs) else
                catalog
        }
    }

    private val tableMetrics: ConcurrentMap<TableIdentifier, IcebergTableMetrics> = ConcurrentHashMap()
    private lateinit var metricsService: MetricsService

    private val channel = SynchronousQueue<CompactionGroup>() // un-buffered BlockingQueue
    private lateinit var rules: CompactionRules
    private lateinit var catalog: Catalog
    private lateinit var namespace: Namespace
    private lateinit var expireRunner: LanePoolRunner<TableIdentifier, TableIdentifier, Unit>
    private lateinit var compactRunner: LanePoolRunner<TableIdentifier, CompactionGroup, Unit>
    private lateinit var compactionGroups: SetMultimap<TableIdentifier, CompactionGroup>
    private lateinit var examineInterval: Duration
    private var expireOrphanSnapshots: Boolean = true

    fun configure(config: Config) {
        metricsService = MetricsService(registry, config.metrics)
            .bindJvmMetrics()
            .bindSystemMetrics()
            .bindAwsClientMetrics()

        catalog = loadCatalog(config.sink.catalog)
        namespace = Namespace.of(*config.manager.namespace)
        examineInterval = config.manager.examineInterval
        rules = config.manager.rules

        expireRunner = LanePoolRunner(executor, this::executeExpiration)
        expireOrphanSnapshots = config.manager.expireOrphanSnapshots
        compactRunner = LanePoolRunner(executor, this::executeCompaction)
        compactRunner.addListener(object : Callback<CompactionGroup, Unit> {
            override fun onSuccess(input: CompactionGroup, result: Unit) {
                if (compactionGroups.remove(input.tableId, input)) {
                    logger.info("Finish compact on {}", input)
                } else {
                    logger.error("Something when wrong, {} has gone", input)
                }
            }

            override fun onFailure(input: CompactionGroup, throwable: Throwable) {
                compactionGroups.remove(input.tableId, input)
                logger.error("Error while compact on {}", input, throwable)
            }
        })

        compactionGroups = Multimaps.newSetMultimap(Maps.newConcurrentMap(), Sets::newHashSet)
    }

    override fun run() {
        metricsService.run()

        val examineIntervalMs = examineInterval.toMillis()
        executor.scheduleWithFixedDelay(::examineTables, 0, examineIntervalMs, MILLISECONDS)

        while (true) {
            val cg = channel.take()
            compactRunner.enqueue(cg.tableId, cg)
        }
    }

    private fun examineTables() {
        val tableIds = catalog.listTables(namespace)
        val now = ZonedDateTime.now(rules.tz)
        val points = CompactionPoints.from(now, rules)

        tableIds.forEach { id ->
            val table = catalog.loadTable(id)

            tableMetrics.computeIfAbsent(id) {
                logger.info("Create new IcebergTableMetrics for {}", it)
                val metrics = IcebergTableMetrics(table)
                metrics.bindTo(registry)
                metrics
            }

            if (expireOrphanSnapshots && !expireRunner.isEmpty(id)) {
                expireRunner.enqueue(id, id)
            }

            CompactionGroup.fromSnapshots(id, table.snapshots(), points::keyOf)
                .filter { it.size > 1 }
                .forEach {
                    if (compactionGroups.put(id, it)) {
                        logger.info("Creating new {}", it)
                        channel.put(it)
                    }
                }
        }
    }

    private fun executeCompaction(cg: CompactionGroup) {
        val table = catalog.loadTable(cg.tableId)
        val action = CompactSnapshots(table, cg.lowSnapshotId, cg.highSnapshotId)
        action.execute()
    }

    private fun executeExpiration(tableId: TableIdentifier) {
        val table = catalog.loadTable(tableId)
        val meta = (table as TableOperations).current()
        val orphanSnapshots = table.snapshots()
            .mapTo(mutableSetOf(), Snapshot::snapshotId)

        // Keep all snapshots that referenced by a branch/tag
        meta.refs().forEach { (_, ref) ->
            table.ancestorsOf(ref.snapshotId()).forEach {
                orphanSnapshots.remove(it.snapshotId())
            }
        }

        if (orphanSnapshots.isEmpty()) return

        val action = table.expireSnapshots()
            .expireOlderThan(Long.MIN_VALUE)
            .cleanExpiredFiles(true)
        orphanSnapshots.forEach(action::expireSnapshotId)
        action.commit()
    }
}
