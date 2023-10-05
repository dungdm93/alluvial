package dev.alluvial.runtime

import dev.alluvial.api.Streamlet.Status.*
import dev.alluvial.schema.debezium.KafkaSchemaTableCreator
import dev.alluvial.sink.iceberg.IcebergSink
import dev.alluvial.source.kafka.KafkaSource
import dev.alluvial.stream.debezium.DebeziumStreamlet
import dev.alluvial.stream.debezium.DebeziumStreamletFactory
import dev.alluvial.utils.ICEBERG_TABLE
import dev.alluvial.utils.KAFKA_TOPIC
import dev.alluvial.utils.recommendedPoolSize
import dev.alluvial.utils.shutdownAndAwaitTermination
import dev.alluvial.utils.withSpan
import io.opentelemetry.api.common.Attributes
import org.apache.iceberg.catalog.TableIdentifier
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.Executors
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.TimeUnit.SECONDS
import kotlin.concurrent.thread

class StreamController : Instrumental(), Runnable {
    companion object {
        private val logger = LoggerFactory.getLogger(StreamController::class.java)
        private val executor = Executors.newScheduledThreadPool(recommendedPoolSize())
    }

    private lateinit var source: KafkaSource
    private lateinit var sink: IcebergSink
    private lateinit var streamletFactory: DebeziumStreamletFactory
    private lateinit var examineInterval: Duration

    private val topic2Table: ConcurrentMap<String, TableIdentifier> = ConcurrentHashMap()
    private val streamlets: ConcurrentMap<String, DebeziumStreamlet> = ConcurrentHashMap()
    private val channel = SynchronousQueue<String>() // un-buffered BlockingQueue

    private val terminatingHook = thread(start = false, name = "terminator") {
        logger.warn("Shutdown Hook: shutdown the ExecutorService")
        executor.shutdownAndAwaitTermination(60, SECONDS)

        logger.warn("Shutdown Hook: closing streamlets")
        streamlets.values.forEach(DebeziumStreamlet::close)
        streamlets.clear()
    }

    fun configure(config: Config) {
        initializeTelemetry(config, "StreamController")

        source = KafkaSource(config.source, telemetry, tracer, meter)
        sink = IcebergSink(config.sink, telemetry, tracer, meter)
        val tableCreator = KafkaSchemaTableCreator(source, sink, config.sink.tableCreation)
        streamletFactory = DebeziumStreamletFactory(config.stream, source, sink, tableCreator, tracer, meter)
        examineInterval = config.stream.examineInterval
    }

    override fun run() {
        Runtime.getRuntime().addShutdownHook(terminatingHook)

        executor.scheduleWithFixedDelay(::examineStreamlets, 0, examineInterval.toMillis(), MILLISECONDS)

        while (true) {
            val topic = channel.take()
            val streamlet = getOrCreateStreamlet(topic)
            logger.info("Launching streamlet {}", streamlet.name)
            executor.submit(streamlet)
        }
    }

    private fun examineStreamlets() = tracer.withSpan("StreamController.examineStreamlets") {
        logger.info("Start examine streamlets")
        val topics = source.availableTopics()

        logger.info("Found {} topics available", topics.size)
        for (topic in topics) {
            examineStreamlet(topic)
        }
    }

    private fun examineStreamlet(topic: String): Unit = tracer.withSpan(
        "StreamController.examineStreamlet"
    ) { span ->
        val tableId = topic2Table.computeIfAbsent(topic) { source.tableIdOf(topic) }
        val attr = Attributes.of(KAFKA_TOPIC, topic, ICEBERG_TABLE, tableId.toString())
        if (topic !in streamlets) {
            if (currentLagOf(topic, tableId) > 0) channel.put(topic)
            span.addEvent("Streamlet.NEW", attr)
            return
        }
        val streamlet = streamlets[topic]!!
        span.setAttribute("alluvial.streamlet", streamlet.name)
        when (streamlet.status) {
            CREATED -> logger.warn("Streamlet {} is still in CREATED state, something may be wrong!!!", streamlet.name)

            RUNNING, COMMITTING -> logger.info("Streamlet {} is {}", streamlet.name, streamlet.status)

            SUSPENDED -> {
                if (streamlet.canTerminate()) {
                    logger.info("Close streamlet {}: No more message for awhile", streamlet.name)
                    streamlet.close()
                    streamlets.remove(topic)
                    span.addEvent("Streamlet.TERMINATED", attr)
                } else if (streamlet.shouldRun()) {
                    channel.put(topic)
                    span.addEvent("Streamlet.RESUMING", attr)
                } else {
                    logger.info("Streamlet {} still SUSPENDED for next examination", streamlet.name)
                }
            }

            FAILED -> logger.error("Streamlet {} is FAILED", streamlet.name) // TODO add retry mechanism
        }
    }

    private fun currentLagOf(topic: String, tableId: TableIdentifier): Long {
        val latestOffsets = source.latestOffsets(topic)
        val committedOffsets = sink.committedBrokerOffsets(tableId)

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
