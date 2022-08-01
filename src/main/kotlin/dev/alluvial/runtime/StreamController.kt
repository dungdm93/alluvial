package dev.alluvial.runtime

import dev.alluvial.api.Streamlet.Status.*
import dev.alluvial.metrics.MetricsService
import dev.alluvial.schema.debezium.KafkaSchemaTableCreator
import dev.alluvial.sink.iceberg.IcebergSink
import dev.alluvial.source.kafka.KafkaSource
import dev.alluvial.stream.debezium.DebeziumStreamlet
import dev.alluvial.stream.debezium.DebeziumStreamletFactory
import dev.alluvial.utils.recommendedPoolSize
import dev.alluvial.utils.shutdownAndAwaitTermination
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Metrics
import org.apache.iceberg.catalog.TableIdentifier
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.Executors
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.TimeUnit.SECONDS
import kotlin.concurrent.thread

class StreamController : Runnable {
    companion object {
        private val logger = LoggerFactory.getLogger(StreamController::class.java)
        private val executor = Executors.newScheduledThreadPool(recommendedPoolSize())
        private val registry = Metrics.globalRegistry
    }

    private val metrics = AppMetrics(registry)
    private lateinit var metricsService: MetricsService

    private lateinit var source: KafkaSource
    private lateinit var sink: IcebergSink
    private lateinit var streamletFactory: DebeziumStreamletFactory
    private lateinit var examineInterval: Duration

    private val topic2Table: ConcurrentMap<String, TableIdentifier> = ConcurrentHashMap()
    private val streamlets: ConcurrentMap<String, DebeziumStreamlet> = ConcurrentHashMap()
    private val channel = SynchronousQueue<String>() // un-buffered BlockingQueue

    private val terminateStreamletsHook = thread(start = false, name = "terminator") {
        logger.warn("Shutdown Hook: shutdown the ExecutorService")
        executor.shutdownAndAwaitTermination(60, SECONDS)

        logger.warn("Shutdown Hook: closing streamlets")
        streamlets.values.forEach(DebeziumStreamlet::close)
        streamlets.clear()

        logger.warn("Shutdown Hook: closing metrics")
        metrics.close()
        metricsService.close()
    }

    fun configure(config: Config) {
        metricsService = MetricsService(registry, config.metric)
            .bindJvmMetrics()
            .bindSystemMetrics()
            .bindAwsClientMetrics()

        source = KafkaSource(config.source, registry)
        sink = IcebergSink(config.sink, registry)
        val tableCreator = KafkaSchemaTableCreator(source, sink, config.sink.tableCreation)
        streamletFactory = DebeziumStreamletFactory(source, sink, tableCreator, config.stream, registry)
        examineInterval = config.stream.examineInterval
    }

    override fun run() {
        metricsService.run()
        Runtime.getRuntime().addShutdownHook(terminateStreamletsHook)

        executor.scheduleWithFixedDelay(::examineStreamlets, 0, examineInterval.toMillis(), MILLISECONDS)

        while (true) {
            val topic = channel.take()
            val streamlet = getOrCreateStreamlet(topic)
            logger.info("Launching streamlet {}", streamlet.name)
            executor.submit(streamlet)
        }
    }

    private fun examineStreamlets() {
        logger.info("Start examine streamlets")
        val topics = source.availableTopics()
        metrics.availableTopicsCount = topics.size

        logger.info("Found {} topics available", topics.size)
        for (topic in topics) {
            examineStreamlet(topic)
        }
    }

    private fun examineStreamlet(topic: String) {
        val tableId = topic2Table.computeIfAbsent(topic) { source.tableIdOf(topic) }
        if (topic !in streamlets) {
            if (currentLagOf(topic, tableId) > 0) channel.put(topic)
            return
        }
        val streamlet = streamlets[topic]!!
        when (streamlet.status) {
            CREATED -> logger.warn("Streamlet {} is still in CREATED state, something may be wrong!!!", streamlet.name)

            RUNNING,
            COMMITTING -> logger.info("Streamlet {} is {}", streamlet.name, streamlet.status)

            SUSPENDED -> {
                if (streamlet.canTerminate()) {
                    logger.info("Close streamlet {}: No more message for awhile", streamlet.name)
                    streamlet.close()
                    streamlets.remove(topic)
                        ?.also(metrics::unregisterStreamlet)
                } else if (streamlet.shouldRun()) {
                    channel.put(topic)
                } else {
                    logger.info("Streamlet {} still SUSPENDED for next examination", streamlet.name)
                }
            }

            FAILED -> logger.error("Streamlet {} is FAILED", streamlet.name) // TODO add retry mechanism
        }
    }

    private fun currentLagOf(topic: String, tableId: TableIdentifier): Long {
        val latestOffsets = source.latestOffsets(topic)
        val committedOffsets = sink.committedOffsets(tableId)

        var lag = 0L
        latestOffsets.forEach { (partition, latestOffset) ->
            val committedPosition = committedOffsets[partition] ?: 0
            lag += latestOffset - committedPosition
        }
        return lag
    }

    private fun getOrCreateStreamlet(topic: String): DebeziumStreamlet {
        val tableId = topic2Table[topic]!!
        return streamlets.computeIfAbsent(topic) {
            logger.info("create new stream {}", it)
            streamletFactory.createStreamlet(topic, tableId)
                .also(metrics::registerStreamlet)
        }
    }

    private inner class AppMetrics(private val registry: MeterRegistry) : Closeable {
        private val streamletStatuses = ConcurrentHashMap<String, Gauge>()
        private val availableTopics = Gauge.builder("alluvial.topic.available", this) {
            it.availableTopicsCount.toDouble()
        }.register(registry)

        var availableTopicsCount = 0

        fun registerStreamlet(streamlet: DebeziumStreamlet) {
            streamletStatuses.computeIfAbsent(streamlet.name) {
                Gauge.builder("alluvial.streamlet.status", streamlet) { it.status.ordinal.toDouble() }
                    .tags("streamlet", streamlet.name)
                    .register(registry)
            }
        }

        fun unregisterStreamlet(streamlet: DebeziumStreamlet) {
            streamletStatuses.remove(streamlet.name)?.let {
                it.close()
                registry.remove(it)
                logger.debug("Unwatch status of streamlet {}", streamlet.name)
            }
        }

        override fun close() {
            availableTopics.close()
            registry.remove(availableTopics)

            streamletStatuses.forEach { (_, meter) ->
                meter.close()
                registry.remove(meter)
            }
            streamletStatuses.clear()
        }
    }
}
