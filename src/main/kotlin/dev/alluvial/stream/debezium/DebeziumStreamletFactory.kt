package dev.alluvial.stream.debezium

import dev.alluvial.api.TableCreator
import dev.alluvial.runtime.StreamConfig
import dev.alluvial.schema.debezium.KafkaSchemaSchemaHandler
import dev.alluvial.sink.iceberg.IcebergSink
import dev.alluvial.sink.iceberg.IcebergTableOutlet
import dev.alluvial.source.kafka.KafkaSource
import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.api.trace.Tracer
import org.apache.iceberg.catalog.TableIdentifier

class DebeziumStreamletFactory(
    private val config: StreamConfig,
    private val source: KafkaSource,
    private val sink: IcebergSink,
    private val tableCreator: TableCreator,
    private val tracer: Tracer,
    private val meter: Meter,
) {
    fun createStreamlet(topic: String, tableId: TableIdentifier): DebeziumStreamlet {
        val name = tableId.toString()

        val inlet = source.getInlet(name, topic)
        val outlet = sink.getOutlet(name, config.connector, tableId) ?: createOutlet(name, topic, tableId)
        val tracker = outlet.tracker
        val schemaHandler = KafkaSchemaSchemaHandler(name, outlet, tracer, meter)

        return DebeziumStreamlet(name, config, inlet, outlet, tracker, schemaHandler, tracer, meter)
    }

    private fun createOutlet(name: String, topic: String, tableId: TableIdentifier): IcebergTableOutlet {
        tableCreator.createTable(topic, tableId)
        return sink.getOutlet(name, config.connector, tableId)!!
    }
}
