package dev.alluvial.stream.debezium

import dev.alluvial.api.TableCreator
import dev.alluvial.runtime.StreamConfig
import dev.alluvial.schema.debezium.KafkaSchemaSchemaHandler
import dev.alluvial.sink.iceberg.IcebergSink
import dev.alluvial.sink.iceberg.IcebergTableOutlet
import dev.alluvial.source.kafka.KafkaSource
import io.micrometer.core.instrument.MeterRegistry
import org.apache.iceberg.catalog.TableIdentifier

class DebeziumStreamletFactory(
    private val source: KafkaSource,
    private val sink: IcebergSink,
    private val tableCreator: TableCreator,
    private val streamConfig: StreamConfig,
    private val registry: MeterRegistry,
) {
    fun createStreamlet(topic: String, tableId: TableIdentifier): DebeziumStreamlet {
        val name = tableId.toString()

        val inlet = source.getInlet(name, topic)
        val outlet = sink.getOutlet(name, tableId) ?: createOutlet(name, topic, tableId)
        val schemaHandler = KafkaSchemaSchemaHandler(outlet)

        return DebeziumStreamlet(name, inlet, outlet, schemaHandler, streamConfig, registry)
    }

    private fun createOutlet(name: String, topic: String, tableId: TableIdentifier): IcebergTableOutlet {
        tableCreator.createTable(topic, tableId)
        return sink.getOutlet(name, tableId)!!
    }
}
