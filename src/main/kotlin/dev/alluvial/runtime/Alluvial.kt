package dev.alluvial.runtime

import dev.alluvial.api.Streamlet.Status.*
import dev.alluvial.api.StreamletId
import dev.alluvial.sink.iceberg.IcebergSink
import dev.alluvial.source.kafka.KafkaSource
import dev.alluvial.stream.debezium.DebeziumStreamlet
import dev.alluvial.stream.debezium.DebeziumStreamletFactory
import dev.alluvial.utils.SystemTime
import dev.alluvial.utils.scheduleInterval
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Runnable
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import kotlin.time.Duration.Companion.minutes

class Alluvial(config: Config) : Runnable {
    companion object {
        private val logger = LoggerFactory.getLogger(Alluvial::class.java)
    }

    private val source = KafkaSource(config.source.props)
    private val sink = IcebergSink(config.sink.props)
    private val streamletFactory = DebeziumStreamletFactory(source, sink)
    private val streamlets: ConcurrentMap<StreamletId, DebeziumStreamlet> = ConcurrentHashMap()
    private val streamletIdleTimeout = 15.minutes.inWholeMilliseconds
    private val time = SystemTime

    override fun run(): Unit = runBlocking {
        val channel = Channel<StreamletId>()

        scheduleInterval(3.minutes, Dispatchers.Default) {
            examineStreamlets(channel)
        }

        supervisorScope {
            for (id in channel) {
                val streamlet = getOrCreateStreamlet(id)
                launch(Dispatchers.IO) {
                    logger.info("Launching streamlet {}", id)
                    streamlet.run()
                }
            }
        }
    }

    private suspend fun examineStreamlets(channel: SendChannel<StreamletId>) {
        logger.info("Start examine streamlets")
        val ids = source.availableStreams()

        logger.info("Found {} streamlet available", ids.size)
        for (id in ids) {
            examineStreamlet(id, channel)
        }
    }

    private suspend fun examineStreamlet(id: StreamletId, channel: SendChannel<StreamletId>) {
        if (id !in streamlets) {
            if (currentLagOf(id) > 0) channel.send(id)
            return
        }
        val streamlet = streamlets[id]!!
        when (streamlet.status) {
            CREATED -> logger.warn("Streamlet {} is still in CREATED state, something may be wrong!!!", id)
            RUNNING -> logger.info("Streamlet {} is RUNNING", id)
            SUSPENDED -> {
                val lag = streamlet.inlet.currentLag()
                val committedTime = streamlet.outlet.committedTimestamp() ?: Long.MIN_VALUE
                if (lag <= 0 && committedTime < time.millis() - streamletIdleTimeout) {
                    logger.info("No more message in {} for {}ms => close streamlet", id, streamletIdleTimeout)
                    streamlet.close()
                    streamlets.remove(id)
                } else if (streamlet.shouldRun()) {
                    channel.send(id)
                } else {
                    logger.info("Streamlet {} still SUSPENDED for next examination", id)
                }
            }
            FAILED -> logger.error("Streamlet {} is FAILED", id) // TODO add retry mechanism
        }
    }

    private fun currentLagOf(id: StreamletId): Long {
        val latestOffsets = source.latestOffsets(id)
        val committedPositions = sink.committedPositions(id)

        var lag = 0L
        latestOffsets.forEach { (partition, latestOffset) ->
            val committedPosition = committedPositions?.get(partition) ?: 0
            lag += latestOffset - committedPosition
        }
        return lag
    }

    private fun getOrCreateStreamlet(id: StreamletId): DebeziumStreamlet {
        return streamlets.computeIfAbsent(id) {
            logger.info("create new stream {}", it)
            streamletFactory.createStreamlet(id)
        }
    }
}
