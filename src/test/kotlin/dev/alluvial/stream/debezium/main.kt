package dev.alluvial.stream.debezium

import dev.alluvial.api.StreamletId
import dev.alluvial.runtime.SinkConfig
import dev.alluvial.runtime.SourceConfig
import dev.alluvial.schema.debezium.KafkaSchemaTableCreator
import dev.alluvial.sink.iceberg.IcebergSink
import dev.alluvial.source.kafka.KafkaSource

fun createSource(): KafkaSource {
    val config = mapOf(
        "bootstrap.servers" to "broker:9092",
        "group.id" to "alluvial",
        "key.converter" to "io.confluent.connect.avro.AvroConverter",
        "value.converter" to "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url" to "http://schema-registry:8081",
        "value.converter.schema.registry.url" to "http://schema-registry:8081",
    )
    val sourceConfig = SourceConfig(
        kind = "kafka",
        topicPrefix = "debezium.mysql",
        config = config
    )
    return KafkaSource(sourceConfig)
}

fun createSink(): IcebergSink {
    val config = mapOf(
        "catalog-type" to "hadoop",
        "warehouse" to "/tmp/warehouse",
    )
    val sinkConfig = SinkConfig(
        kind = "iceberg",
        catalog = config
    )
    return IcebergSink(sinkConfig)
}

fun main() {
    val id = StreamletId("sakila", "film")
    val source = createSource()
    val sink = createSink()
    val tableCreator = KafkaSchemaTableCreator(source, sink)
    val streamletFactory = DebeziumStreamletFactory(source, sink, tableCreator)
    val streamlet = streamletFactory.createStreamlet(id)

    streamlet.run()
}
