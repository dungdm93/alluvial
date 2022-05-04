package dev.alluvial.source.kafka

import dev.alluvial.api.StreamletId
import dev.alluvial.runtime.SourceConfig
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory


@Suppress("MemberVisibilityCanBePrivate")
class KafkaSource(sourceConfig: SourceConfig) {
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

    private val config = sourceConfig.config + DEFAULT_CONFIG
    private val topicPrefix = sourceConfig.topicPrefix
    private val adminClient = Admin.create(config)
    private val converter = KafkaConverter(config)

    fun availableStreams(): List<StreamletId> {
        val topics = adminClient.listTopics().names().get()
        return topics.filter { it.startsWith("${topicPrefix}.") }
            .mapNotNull(::idOf)
    }

    fun getInlet(id: StreamletId): KafkaTopicInlet {
        val topic = topicOf(id)
        val consumer = newConsumer<ByteArray, ByteArray>()
        val converter = getConverter()
        return KafkaTopicInlet(id, topic, consumer, converter)
    }

    fun latestOffsets(id: StreamletId): Map<Int, Long> {
        val topic = topicOf(id)
        val partitions = adminClient.describeTopics(listOf(topic))
            .all().get()[topic]!!
            .partitions()

        val request = partitions.associate { TopicPartition(topic, it.partition()) to OffsetSpec.latest() }
        val tp2ori = adminClient.listOffsets(request)
            .all().get()

        return tp2ori.map { (tp, oam) ->
            tp.partition() to oam.offset()
        }.toMap()
    }

    fun <K, V> newConsumer(overrideConfig: Map<String, Any>? = null): KafkaConsumer<K, V> {
        val config = if (overrideConfig == null)
            this.config else
            this.config + overrideConfig
        return KafkaConsumer(config)
    }

    fun getConverter(): KafkaConverter {
        return converter
    }

    fun idOf(topic: String): StreamletId? {
        val parts = topic.removePrefix("${topicPrefix}.")
            .split(".")
        if (parts.size != 2) {
            logger.warn("Ignore topic {}: not matching with pattern {}.<schemaName>.<tableName>", topic, topicPrefix)
            return null
        }
        return StreamletId(parts[0], parts[1])
    }

    fun topicOf(id: StreamletId): String {
        return "${topicPrefix}.${id.schema}.${id.table}"
    }
}
