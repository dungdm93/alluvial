package dev.alluvial.stream.debezium

import dev.alluvial.api.StreamletId
import dev.alluvial.api.TableCreator
import dev.alluvial.schema.debezium.KafkaSchemaSchemaHandler
import dev.alluvial.runtime.StreamConfig
import dev.alluvial.sink.iceberg.IcebergSink
import dev.alluvial.sink.iceberg.IcebergTableOutlet
import dev.alluvial.source.kafka.KafkaSource

class DebeziumStreamletFactory(
    private val source: KafkaSource,
    private val sink: IcebergSink,
    private val tableCreator: TableCreator,
    private val streamConfig: StreamConfig,
) {
    fun createStreamlet(id: StreamletId): DebeziumStreamlet {
        val inlet = source.getInlet(id)
        val outlet = sink.getOutlet(id) ?: createOutlet(id)
        val schemaHandler = KafkaSchemaSchemaHandler(id, outlet)

        return DebeziumStreamlet(id, inlet, outlet, schemaHandler, streamConfig)
    }

    private fun createOutlet(id: StreamletId): IcebergTableOutlet {
        tableCreator.createTable(id)
        return sink.getOutlet(id)!!
    }
}
