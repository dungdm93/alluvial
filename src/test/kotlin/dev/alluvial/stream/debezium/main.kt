package dev.alluvial.stream.debezium

import dev.alluvial.runtime.Config
import dev.alluvial.runtime.MetricConfig
import dev.alluvial.runtime.SinkConfig
import dev.alluvial.runtime.SourceConfig
import dev.alluvial.runtime.StreamConfig
import dev.alluvial.runtime.TableCreationConfig
import dev.alluvial.schema.debezium.KafkaSchemaTableCreator
import dev.alluvial.sink.iceberg.IcebergSink
import dev.alluvial.source.kafka.KafkaSource
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier

val config = Config(
    source = SourceConfig(
        kind = "kafka",
        topicPrefix = "debezium.mysql",
        config = mapOf(
            "bootstrap.servers" to "broker:9092",
            "group.id" to "alluvial",
            "key.converter" to "io.confluent.connect.avro.AvroConverter",
            "value.converter" to "io.confluent.connect.avro.AvroConverter",
            "key.converter.schema.registry.url" to "http://schema-registry:8081",
            "value.converter.schema.registry.url" to "http://schema-registry:8081",
        )
    ),
    sink = SinkConfig(
        kind = "iceberg",
        catalog = mapOf(
            "catalog-type" to "hadoop",
            "warehouse" to "/tmp/warehouse",
        ),
        tableCreation = TableCreationConfig(),
    ),
    stream = StreamConfig(kind = "debezium"),
    metric = MetricConfig(kind = "prometheus"),
)

fun main() {
    val source = KafkaSource(config.source)
    val sink = IcebergSink(config.sink)
    val tableCreator = KafkaSchemaTableCreator(source, sink, config.sink.tableCreation)
    val streamletFactory = DebeziumStreamletFactory(source, sink, tableCreator, config.stream)

    val topic = "debezium.mysql.sakila.sakila.film"
    val tableId = TableIdentifier.of(Namespace.of("dvdrental"), "film")
    val streamlet = streamletFactory.createStreamlet(topic, tableId)

    streamlet.run()
}
