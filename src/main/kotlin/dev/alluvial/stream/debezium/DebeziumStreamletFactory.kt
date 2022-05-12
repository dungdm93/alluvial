package dev.alluvial.stream.debezium

import dev.alluvial.api.TableCreator
import dev.alluvial.runtime.StreamConfig
import dev.alluvial.schema.debezium.KafkaSchemaSchemaHandler
import dev.alluvial.sink.iceberg.IcebergSink
import dev.alluvial.sink.iceberg.IcebergTableOutlet
import dev.alluvial.source.kafka.KafkaSource
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

        val id = tableId.toString()
        return DebeziumStreamlet(id, inlet, outlet, schemaHandler, streamConfig)
    }

    private fun createOutlet(topic: String, tableId: TableIdentifier): IcebergTableOutlet {
        tableCreator.createTable(topic, tableId)
        return sink.getOutlet(tableId)!!
    }
}
