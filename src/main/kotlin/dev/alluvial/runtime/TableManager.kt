package dev.alluvial.runtime

import com.google.common.collect.Maps
import com.google.common.collect.Multimaps
import com.google.common.collect.SetMultimap
import com.google.common.collect.Sets
import dev.alluvial.utils.Callback
import dev.alluvial.utils.CompactionGroup
import dev.alluvial.utils.CompactionPoints
import dev.alluvial.utils.ICEBERG_TABLE
import dev.alluvial.utils.LanePoolRunner
import dev.alluvial.utils.recommendedPoolSize
import dev.alluvial.utils.schedule
import dev.alluvial.utils.shutdownAndAwaitTermination
import dev.alluvial.utils.withSpan
import io.opentelemetry.api.common.Attributes
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.CachingCatalog
import org.apache.iceberg.CatalogProperties
import org.apache.iceberg.CatalogUtil
import org.apache.iceberg.CompactSnapshots
import org.apache.iceberg.HasTableOperations
import org.apache.iceberg.PendingUpdate
import org.apache.iceberg.Snapshot
import org.apache.iceberg.Table
import org.apache.iceberg.ancestorsOf
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.currentAncestors
import org.apache.iceberg.sourceTimestampMillis
import org.apache.iceberg.util.PropertyUtil
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.time.Duration
import java.time.ZonedDateTime
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

class TableManager : Instrumental(), Runnable {
    companion object {
        private val logger = LoggerFactory.getLogger(TableManager::class.java)
        private val executor = Executors.newScheduledThreadPool(recommendedPoolSize())

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

    private lateinit var compactionConfig: CompactionConfig
    private lateinit var expirationConfig: ExpirationConfig
    private lateinit var catalog: Catalog
    private lateinit var namespace: Namespace
    private lateinit var tagRunner: LanePoolRunner<TableIdentifier, TableIdentifier, Unit>
    private lateinit var expireRunner: LanePoolRunner<TableIdentifier, TableIdentifier, Unit>
    private lateinit var compactRunner: LanePoolRunner<TableIdentifier, CompactionGroup, Unit>
    private lateinit var compactionGroups: SetMultimap<TableIdentifier, CompactionGroup>
    private lateinit var examineInterval: Duration

    private val terminatingHook = thread(start = false, name = "terminator") {
        logger.warn("Shutdown Hook: shutdown the ExecutorService")
        executor.shutdownAndAwaitTermination(60, TimeUnit.SECONDS)
    }

    fun configure(config: Config) {
        initializeTelemetry(config, "TableManager")

        catalog = loadCatalog(config.sink.catalog)
        namespace = Namespace.of(*config.manager.namespace)
        examineInterval = config.manager.examineInterval

        tagRunner = LanePoolRunner(executor, this::executeTagging)
        tagRunner.addListener(newMdcListener("tag", "t"))

        expirationConfig = config.manager.expireOrphanSnapshots
        expireRunner = LanePoolRunner(executor, this::executeExpiration)
        expireRunner.addListener(newMdcListener("expire", "x"))

        compactionConfig = config.manager.compactSnapshots
        compactionGroups = Multimaps.newSetMultimap(Maps.newConcurrentMap(), Sets::newHashSet)
        compactRunner = LanePoolRunner(executor, this::executeCompaction)
        compactRunner.addListener(object : Callback<CompactionGroup, Unit> {
            override fun beforeExecute(input: CompactionGroup) {
                MDC.put("name", "z(${input.tableId}/${input.key})")
            }

            override fun onSuccess(input: CompactionGroup, result: Unit) {
                if (compactionGroups.remove(input.tableId, input)) {
                    logger.info("Finish compact on {}", input)
                } else {
                    logger.error("Something when wrong, {} has gone", input)
                }
                MDC.remove("name")
            }

            override fun onFailure(input: CompactionGroup, throwable: Throwable) {
                compactionGroups.remove(input.tableId, input)
                logger.error("Error while compact on {}", input, throwable)
                MDC.remove("name")
            }
        })
    }

    override fun run() {
        Runtime.getRuntime().addShutdownHook(terminatingHook)
        schedule(examineInterval, ::examineTables)
    }

    private fun examineTables() = tracer.withSpan("TableManager.examineTables") { span ->
        logger.info("Start examine tables")
        val tableIds = tracer.withSpan("Iceberg.listTables") { catalog.listTables(namespace) }
        val now = ZonedDateTime.now(compactionConfig.tz)
        val points = CompactionPoints.from(now, compactionConfig)

        tableIds.forEach { id ->
            logger.info("Examine table {}", id)
            val table = id.loadTable()
            val attrs = Attributes.of(ICEBERG_TABLE, id.toString())

            if (tagRunner.isEmpty(id)) {
                logger.info("Enqueue taggingSnapshots for {}", id)
                span.addEvent("TableManager.TAG", attrs)
                tagRunner.enqueue(id, id)
            }

            if (expirationConfig.enabled && expireRunner.isEmpty(id)) {
                logger.info("Enqueue expireSnapshots for {}", id)
                span.addEvent("TableManager.EXPIRE", attrs)
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
                    span.addEvent("TableManager.COMPACT", attrs)
                    compactRunner.enqueue(it.tableId, it)
                }
            }
        }
    }

    private fun executeCompaction(cg: CompactionGroup) = tracer.withSpan(
        "TableManager.executeCompaction",
        Attributes.of(ICEBERG_TABLE, cg.tableId.toString()),
    ) {
        logger.info("Run compaction on {}", cg)
        val table = cg.tableId.loadTable()
        val action = CompactSnapshots(cg.lowSnapshotId, cg.highSnapshotId, table, tracer, meter)
        action.execute()
    }

    private fun executeExpiration(tableId: TableIdentifier) = tracer.withSpan(
        "TableManager.executeExpiration",
        Attributes.of(ICEBERG_TABLE, tableId.toString()),
    ) {
        logger.info("Run expiration on {}", tableId)
        val table = tableId.loadTable()
        val meta = (table as HasTableOperations).operations().current()
        val expireTime = System.currentTimeMillis() - expirationConfig.age.toMillis()
        val orphanSnapshots = table.snapshots()
            .mapTo(mutableSetOf(), Snapshot::snapshotId)

        // Keep all snapshots that referenced by a branch/tag
        meta.refs().forEach { (_, ref) ->
            table.ancestorsOf(ref.snapshotId())
                .forEach {
                    orphanSnapshots.remove(it.snapshotId())
                }
        }

        // Keep all recent created snapshots
        orphanSnapshots.removeIf {
            val snapshot = table.snapshot(it)
            snapshot.timestampMillis() < expireTime
        }

        if (orphanSnapshots.isEmpty()) return

        val action = table.expireSnapshots()
            .expireOlderThan(Long.MIN_VALUE)
            .cleanExpiredFiles(true)
        orphanSnapshots.forEach(action::expireSnapshotId)
        action.commitUpdate()
    }

    private fun executeTagging(tableId: TableIdentifier) = tracer.withSpan(
        "TableManager.executeTagging",
        Attributes.of(ICEBERG_TABLE, tableId.toString()),
    ) {
        logger.info("Run tagging on {}", tableId)
        val now = ZonedDateTime.now(compactionConfig.tz)
        val points = CompactionPoints.from(now, compactionConfig)
        val taggingPoint = points.dayCompactionPoint.toEpochMilli()

        val table = tableId.loadTable()
        val refs = (table as HasTableOperations).operations().current().refs()

        val cgs = CompactionGroup
            .fromSnapshots(tableId, table.currentAncestors(), points::keyOf)
            .reversed()

        val manageSnapshots = table.manageSnapshots()
        for (cg in cgs) {
            if (cg.size > 1) break
            val snapshot = table.snapshot(cg.highSnapshotId)
            if (taggingPoint <= snapshot.sourceTimestampMillis()!!) break
            // In the case of catch-up run, check source timestamp is not enough.
            // This is the easiest way to check cg.key only contains day part.
            if (cg.key.contains('T')) break

            if (cg.key in refs) continue
            logger.info("Creating tag {} from {}", cg.key, cg.highSnapshotId)
            manageSnapshots.createTag(cg.key, cg.highSnapshotId)
        }
        manageSnapshots.commitUpdate()
    }

    @Suppress("NOTHING_TO_INLINE")
    private inline fun TableIdentifier.loadTable(): Table {
        return tracer.withSpan("Iceberg.loadTable") { catalog.loadTable(this) }
    }

    @Suppress("NOTHING_TO_INLINE")
    private inline fun PendingUpdate<*>.commitUpdate() {
        return tracer.withSpan("Iceberg.commit") { this.commit() }
    }

    private fun <I, O> newMdcListener(action: String, code: String): Callback<I, O> {
        return object : Callback<I, O> {
            override fun beforeExecute(input: I) {
                MDC.put("name", "$code($input)")
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
