package dev.alluvial.source.kafka

import dev.alluvial.runtime.SourceConfig
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig


fun main() {
    val config: Map<String, String> = mapOf(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.ByteArraySerializer",
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.ByteArraySerializer",

        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG to "broker:9092",
        CommonClientConfigs.GROUP_ID_CONFIG to "foobar",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
        "key.converter" to "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url" to "http://schema-registry:8081",
        "value.converter" to "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url" to "http://schema-registry:8081",
    )
    val sourceConfig = SourceConfig(
        kind = "kafka",
        topicPrefix = "debezium.mysql",
        config = config
    )
    val source = KafkaSource(sourceConfig)
    val inlet = source.getInlet("debezium.mysql.sakila.sakila.actor")

    repeat(100) {
        val record = inlet.read()
        println(record)
    }
}
