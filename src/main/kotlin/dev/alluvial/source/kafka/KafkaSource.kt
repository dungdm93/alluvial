package dev.alluvial.source.kafka

import dev.alluvial.runtime.SourceConfig
import dev.alluvial.source.kafka.naming.NamingAdjusterManager
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.instrumentation.kafkaclients.v2_6.KafkaTelemetry
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.io.Closeable

class KafkaSource(
    sourceConfig: SourceConfig,
    telemetry: OpenTelemetry,
    private val tracer: Tracer,
    private val meter: Meter,
) {
    companion object {
        private val logger = LoggerFactory.getLogger(KafkaSource::class.java)
        private val DEFAULT_CONFIG = mapOf(
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.ByteArraySerializer",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.ByteArrayDeserializer",
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.ByteArraySerializer",
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        )
    }

    private val kafkaTelemetry = KafkaTelemetry.create(telemetry)
    private val config = buildMap {
        putAll(sourceConfig.config)
        // TracingInterceptor classes SHOULD accept config opentelemetry.supplier
        put(
            ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
            "io.opentelemetry.instrumentation.kafkaclients.v2_6.TracingProducerInterceptor"
        )
        put(
            ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
            "io.opentelemetry.instrumentation.kafkaclients.v2_6.TracingConsumerInterceptor"
        )
        putAll(kafkaTelemetry.metricConfigProperties())
        putAll(DEFAULT_CONFIG)
    }

    private val topicPrefix = sourceConfig.topicPrefix.trimEnd('.') + "."
    private val topicsExcluded = sourceConfig.topicsExcluded
    private val topicsIncluded = sourceConfig.topicsIncluded
    private val pollTimeout = sourceConfig.pollTimeout
    private val adminClient = AdminClient.create(config)
    private val converter = KafkaConverter(config)
    private val naming = NamingAdjusterManager(sourceConfig.namingAdjusters)

    fun availableTopics(): List<String> {
        val topics = adminClient.listTopics().names().get()
        return topics.filter {
            if (topicsIncluded.isNotEmpty())
                return@filter topicsIncluded.contains(it)
            if (!it.startsWith(topicPrefix) || topicsExcluded.contains(it)) return@filter false
            val str = it.substring(topicPrefix.length)
            str.contains('.')
        }
    }

    fun getInlet(name: String, topic: String): KafkaTopicInlet {
        val overrideConfig = mapOf(ConsumerConfig.CLIENT_ID_CONFIG to name)
        val consumer = newConsumer<ByteArray?, ByteArray?>(overrideConfig)
        val converter = getConverter()

        logger.info("Creating new inlet {}", name)
        return KafkaTopicInlet(name, topic, consumer, converter, pollTimeout, tracer, meter)
    }

    fun latestOffsets(topic: String): Map<Int, Long> {
        val partitions = adminClient.describeTopics(listOf(topic))
            .allTopicNames().get()[topic]!!
            .partitions()

        val request = partitions.associate { TopicPartition(topic, it.partition()) to OffsetSpec.latest() }
        val tp2ori = adminClient.listOffsets(request)
            .all().get()

        return tp2ori.map { (tp, oam) ->
            tp.partition() to oam.offset()
        }.toMap()
    }

    fun <K, V> newConsumer(overrideConfig: Map<String, *>? = null): KafkaConsumer<K, V> {
        val config = if (overrideConfig == null)
            this.config else
            this.config + overrideConfig
        return KafkaConsumer(config)
    }

    fun getConverter(): KafkaConverter {
        return converter
    }

    fun tableIdOf(topic: String): TableIdentifier {
        val parts = topic.removePrefix(topicPrefix)
            .split(".")
        if (parts.size < 2) {
            val msg = "topic $topic: not matching with pattern $topicPrefix<schemaName>.<tableName>"
            throw IllegalArgumentException(msg)
        }

        var table = parts.last()
        var ns = parts.dropLast(1)
        ns = naming.adjustNamespace(ns)
        table = naming.adjustTable(ns, table)

        return TableIdentifier.of(*ns.toTypedArray(), table)
    }
}
