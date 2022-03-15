package dev.alluvial.stream.debezium

import dev.alluvial.api.StreamletId
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
        KafkaSource.TOPIC_PREFIX_PROP to "debezium.mysql",
    )
    return KafkaSource(config)
}

fun createSink(): IcebergSink {
    val config = mapOf(
        "catalog-type" to "hadoop",
        "warehouse" to "/tmp/warehouse",
    )
    return IcebergSink(config)
}

fun main() {
    val id = StreamletId("sakila", "film")
    val source = createSource()
    val sink = createSink()
    val streamletFactory = DebeziumStreamletFactory(source, sink)
    val streamlet = streamletFactory.createStreamlet(id)

    streamlet.run()
}
