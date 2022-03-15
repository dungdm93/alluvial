package dev.alluvial.source.kafka

import dev.alluvial.api.StreamletId
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory


@Suppress("MemberVisibilityCanBePrivate")
class KafkaSource(config: Map<String, Any>) {
    companion object {
        private val logger = LoggerFactory.getLogger(KafkaSource::class.java)

        const val TOPIC_PREFIX_PROP = "alluvial.source.kafka.topic-prefix"
    }

    private val config: Map<String, Any>
    private val adminClient: Admin
    private val consumerGroupId: String
    private val topicPrefix: String

    init {
        this.config = config + mapOf(
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to "false",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.ByteArraySerializer",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.ByteArrayDeserializer",
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.ByteArraySerializer",
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        )
        this.topicPrefix = (config[TOPIC_PREFIX_PROP] as String).removeSuffix(".")
        this.adminClient = Admin.create(config)
        this.consumerGroupId = config[CommonClientConfigs.GROUP_ID_CONFIG] as String
    }

    fun availableStreams(): List<StreamletId> {
        val topics = adminClient.listTopics().names().get()
        return topics.filter { it.startsWith("${topicPrefix}.") }
            .mapNotNull(::idOf)
    }

    fun getInlet(id: StreamletId): KafkaTopicInlet {
        val topic = topicOf(id)
        return KafkaTopicInlet(id, topic, config)
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

    fun createConsumer(): KafkaConsumer<ByteArray, ByteArray> {
        return KafkaConsumer<ByteArray, ByteArray>(config)
    }

    fun createConverter(): KafkaConverter {
        return KafkaConverter(config)
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
