package dev.alluvial.runtime

import dev.alluvial.api.Streamlet.Status.*
import dev.alluvial.metric.MetricService
import dev.alluvial.schema.debezium.KafkaSchemaTableCreator
import dev.alluvial.sink.iceberg.IcebergSink
import dev.alluvial.source.kafka.KafkaSource
import dev.alluvial.stream.debezium.DebeziumStreamlet
import dev.alluvial.stream.debezium.DebeziumStreamletFactory
import dev.alluvial.utils.scheduleInterval
import io.micrometer.core.instrument.Metrics
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Runnable
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import org.apache.iceberg.catalog.TableIdentifier
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import kotlin.concurrent.thread

class Alluvial : Runnable {
    companion object {
        private val logger = LoggerFactory.getLogger(Alluvial::class.java)
    }

    private lateinit var metricService: MetricService
    private lateinit var source: KafkaSource
    private lateinit var sink: IcebergSink
    private lateinit var streamletFactory: DebeziumStreamletFactory
    private lateinit var examineInterval: Duration
    private val streamlets: ConcurrentMap<String, DebeziumStreamlet> = ConcurrentHashMap()
    private val topic2Table = mutableMapOf<String, TableIdentifier>()

    private val terminateStreamletsHook = thread(start = false, name = "terminator") {
        logger.warn("Shutdown Hook: closing streamlets")
        streamlets.values.forEach(DebeziumStreamlet::close)
        streamlets.clear()

        logger.warn("Shutdown Hook: closing metrics")
        metricService.close()
    }

    fun configure(config: Config) {
        metricService = MetricService(Metrics.globalRegistry, config.metric)
            .bindJvmMetrics()
            .bindSystemMetrics()
        val registry = metricService.registry

        source = KafkaSource(config.source, registry)
        sink = IcebergSink(config.sink, registry)
        val tableCreator = KafkaSchemaTableCreator(source, sink, config.sink.tableCreation)
        streamletFactory = DebeziumStreamletFactory(source, sink, tableCreator, config.stream, registry)
        examineInterval = config.stream.examineInterval
    }

    override fun run(): Unit = runBlocking {
        metricService.run()
        Runtime.getRuntime().addShutdownHook(terminateStreamletsHook)

        val channel = Channel<String>()

        scheduleInterval(examineInterval.toMillis(), Dispatchers.Default) {
            examineStreamlets(channel)
        }

        supervisorScope {
            for (topic in channel) {
                val streamlet = getOrCreateStreamlet(topic)
                launch(Dispatchers.IO) {
                    logger.info("Launching streamlet {}", streamlet.name)
                    streamlet.run()
                }
            }
        }
    }

    private suspend fun examineStreamlets(channel: SendChannel<String>) {
        logger.info("Start examine streamlets")
        val topics = source.availableTopics()

        logger.info("Found {} topics available", topics.size)
        for (topic in topics) {
            examineStreamlet(topic, channel)
        }
    }

    private suspend fun examineStreamlet(topic: String, channel: SendChannel<String>) {
        val tableId = topic2Table.computeIfAbsent(topic) { source.tableIdOf(topic) }
        if (topic !in streamlets) {
            if (currentLagOf(topic, tableId) > 0) channel.send(topic)
            return
        }
        val streamlet = streamlets[topic]!!
        when (streamlet.status) {
            CREATED -> logger.warn("Streamlet {} is still in CREATED state, something may be wrong!!!", streamlet.name)
            RUNNING -> logger.info("Streamlet {} is RUNNING", streamlet.name)
            SUSPENDED -> {
                if (streamlet.canTerminate()) {
                    logger.info("Close streamlet {}: No more message for awhile", streamlet.name)
                    streamlet.close()
                    streamlets.remove(topic)
                } else if (streamlet.shouldRun()) {
                    channel.send(topic)
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
        }
    }
}
