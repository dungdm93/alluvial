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
import dev.alluvial.utils.schedule
import dev.alluvial.utils.shutdownAndAwaitTermination
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.binder.iceberg.IcebergTableMetrics
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.CachingCatalog
import org.apache.iceberg.CatalogProperties
import org.apache.iceberg.CatalogUtil
import org.apache.iceberg.CompactSnapshots
import org.apache.iceberg.HasTableOperations
import org.apache.iceberg.Snapshot
import org.apache.iceberg.ancestorsOf
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.currentAncestors
import org.apache.iceberg.util.PropertyUtil
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.time.Duration
import java.time.ZonedDateTime
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MINUTES
import kotlin.concurrent.thread

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
    private val compactMetrics: ConcurrentMap<TableIdentifier, CompactSnapshots.Metrics> = ConcurrentHashMap()
    private lateinit var metricsService: MetricsService

    private lateinit var rules: CompactionRules
    private lateinit var catalog: Catalog
    private lateinit var namespace: Namespace
    private lateinit var tagRunner: LanePoolRunner<TableIdentifier, TableIdentifier, Unit>
    private lateinit var expireRunner: LanePoolRunner<TableIdentifier, TableIdentifier, Unit>
    private lateinit var compactRunner: LanePoolRunner<TableIdentifier, CompactionGroup, Unit>
    private lateinit var compactionGroups: SetMultimap<TableIdentifier, CompactionGroup>
    private lateinit var examineInterval: Duration
    private var expireOrphanSnapshots: Boolean = true

    private val terminatingHook = thread(start = false, name = "terminator") {
        logger.warn("Shutdown Hook: shutdown the ExecutorService")
        executor.shutdownAndAwaitTermination(60, TimeUnit.SECONDS)

        logger.warn("Shutdown Hook: closing metrics")
        metricsService.close()
    }

    fun configure(config: Config) {
        metricsService = MetricsService(registry, config.metrics)
            .bindJvmMetrics()
            .bindSystemMetrics()
            .bindAwsClientMetrics()

        catalog = loadCatalog(config.sink.catalog)
        namespace = Namespace.of(*config.manager.namespace)
        examineInterval = config.manager.examineInterval
        rules = config.manager.rules

        tagRunner = LanePoolRunner(executor, this::executeTagging)
        tagRunner.addListener(newMdcListener("tagging"))

        expireOrphanSnapshots = config.manager.expireOrphanSnapshots
        expireRunner = LanePoolRunner(executor, this::executeExpiration)
        expireRunner.addListener(newMdcListener("expiration"))

        compactionGroups = Multimaps.newSetMultimap(Maps.newConcurrentMap(), Sets::newHashSet)
        compactRunner = LanePoolRunner(executor, this::executeCompaction)
        compactRunner.addListener(object : Callback<CompactionGroup, Unit> {
            override fun beforeExecute(input: CompactionGroup) {
                MDC.put("name", "compaction(${input.tableId}/${input.key})")
            }

            override fun onSuccess(input: CompactionGroup, result: Unit) {
                if (compactionGroups.remove(input.tableId, input)) {
                    logger.info("Finish compact on {}", input)
                } else {
                    logger.error("Something when wrong, {} has gone", input)
                }
                compactMetrics[input.tableId]?.increment(true)
                MDC.remove("name")
            }

            override fun onFailure(input: CompactionGroup, throwable: Throwable) {
                compactionGroups.remove(input.tableId, input)
                logger.error("Error while compact on {}", input, throwable)
                compactMetrics[input.tableId]?.increment(false)
                MDC.remove("name")
            }
        })
    }

    override fun run() {
        metricsService.run()
        Runtime.getRuntime().addShutdownHook(terminatingHook)

        executor.scheduleWithFixedDelay(::refreshMonitors, 0, 1, MINUTES)
        schedule(examineInterval, ::examineTables)
    }

    private fun refreshMonitors() {
        val tableIds = catalog.listTables(namespace)

        tableIds.forEach { id ->
            val table = catalog.loadTable(id)

            tableMetrics.computeIfAbsent(id) {
                logger.info("Create new IcebergTableMetrics for {}", it)
                val metrics = IcebergTableMetrics(table)
                metrics.bindTo(registry)
                metrics
            }
        }
    }

    private fun examineTables() {
        logger.info("Start examine tables")
        val tableIds = catalog.listTables(namespace)
        val now = ZonedDateTime.now(rules.tz)
        val points = CompactionPoints.from(now, rules)

        tableIds.forEach { id ->
            logger.info("Examine table {}", id)
            val table = catalog.loadTable(id)
            compactMetrics.computeIfAbsent(id) { CompactSnapshots.Metrics(registry, id) }

            if (tagRunner.isEmpty(id)) {
                logger.info("Enqueue taggingSnapshots for {}", id)
                tagRunner.enqueue(id, id)
            }

            if (expireOrphanSnapshots && expireRunner.isEmpty(id)) {
                logger.info("Enqueue expireSnapshots for {}", id)
                expireRunner.enqueue(id, id)
            }

            var cgs = CompactionGroup.fromSnapshots(id, table.currentAncestors(), points::keyOf)
                .filter { it.size > 1 }
            val runningItem = compactRunner.runningItem(id)
            if (runningItem != null) {
                // All CompactionGroup has highSequenceNumber >= runningItem.highSequenceNumber will cause an exception
                // Should be filtered them out and waiting for next examine
                cgs = cgs.filter { it < runningItem }
            }

            cgs.forEach {
                if (compactionGroups.put(id, it)) {
                    logger.info("Enqueue compactSnapshots for {}", it)
                    compactRunner.enqueue(it.tableId, it)
                }
            }
        }
    }

    private fun executeCompaction(cg: CompactionGroup) {
        logger.info("Run compaction on {}", cg)
        val table = catalog.loadTable(cg.tableId)
        val metrics = compactMetrics[cg.tableId]!!
        val action = CompactSnapshots(table, metrics, cg.lowSnapshotId, cg.highSnapshotId)
        action.execute()
    }

    private fun executeExpiration(tableId: TableIdentifier) {
        logger.info("Run expiration on {}", tableId)
        val table = catalog.loadTable(tableId)
        val meta = (table as HasTableOperations).operations().current()
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

    private fun executeTagging(tableId: TableIdentifier) {
        logger.info("Run tagging on {}", tableId)
        val now = ZonedDateTime.now(rules.tz)
        val points = CompactionPoints.from(now, rules)
        val taggingPoint = points.dayCompactionPoint.toEpochMilli()

        val table = catalog.loadTable(tableId)
        val refs = (table as HasTableOperations).operations().current().refs()

        val cgs = CompactionGroup
            .fromSnapshots(tableId, table.currentAncestors(), points::keyOf)
            .reversed()

        val manageSnapshots = table.manageSnapshots()
        for (cg in cgs) {
            if (cg.size > 1) break
            val snapshot = table.snapshot(cg.highSnapshotId)
            if (snapshot.timestampMillis() > taggingPoint) break

            if (cg.key in refs) continue
            logger.info("Creating tag {} from {}", cg.key, cg.highSnapshotId)
            manageSnapshots.createTag(cg.key, cg.highSnapshotId)
        }
        manageSnapshots.commit()
    }

    private fun <I, O> newMdcListener(action: String): Callback<I, O> {
        return object : Callback<I, O> {
            override fun beforeExecute(input: I) {
                MDC.put("name", "$action($input)")
            }

            override fun onSuccess(input: I, result: O) {
                logger.info("Finish {} on {}", action, input)
                MDC.remove("name")
            }

            override fun onFailure(input: I, throwable: Throwable) {
                logger.error("Error while {} on {}", action, input, throwable)
                MDC.remove("name")
            }
        }
    }
}
