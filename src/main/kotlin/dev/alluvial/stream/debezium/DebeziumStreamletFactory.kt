package dev.alluvial.stream.debezium

import dev.alluvial.api.TableCreator
import dev.alluvial.runtime.StreamConfig
import dev.alluvial.schema.debezium.KafkaSchemaSchemaHandler
import dev.alluvial.sink.iceberg.IcebergSink
import dev.alluvial.sink.iceberg.IcebergTableOutlet
import dev.alluvial.source.kafka.KafkaSource
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Tag
import org.apache.iceberg.catalog.TableIdentifier

class DebeziumStreamletFactory(
    private val source: KafkaSource,
    private val sink: IcebergSink,
    private val tableCreator: TableCreator,
    private val streamConfig: StreamConfig,
) {
    fun createStreamlet(topic: String, tableId: TableIdentifier): DebeziumStreamlet {
        val inlet = source.getInlet(topic)
        val outlet = sink.getOutlet(tableId) ?: createOutlet(topic, tableId)
        val schemaHandler = KafkaSchemaSchemaHandler(outlet)

        val name = tableId.toString()
        val streamlet = DebeziumStreamlet(name, inlet, outlet, schemaHandler, streamConfig)

        // Metrics
        val registry = Metrics.globalRegistry
        val tags = listOf(Tag.of("streamlet", name))
        inlet.enableMetrics(registry, tags)
        streamlet.enableMetrics(registry, tags)

        return streamlet
    }

    private fun createOutlet(topic: String, tableId: TableIdentifier): IcebergTableOutlet {
        tableCreator.createTable(topic, tableId)
        return sink.getOutlet(tableId)!!
    }
}
