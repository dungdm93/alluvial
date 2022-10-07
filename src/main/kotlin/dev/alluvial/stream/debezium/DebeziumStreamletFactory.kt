package dev.alluvial.stream.debezium

import dev.alluvial.api.TableCreator
import dev.alluvial.dedupe.backend.rocksdb.RocksDbClient
import dev.alluvial.dedupe.backend.rocksdb.RocksDbDeduperProvider
import dev.alluvial.runtime.StreamConfig
import dev.alluvial.schema.debezium.KafkaSchemaSchemaHandler
import dev.alluvial.sink.iceberg.IcebergSink
import dev.alluvial.sink.iceberg.IcebergTableOutlet
import dev.alluvial.source.kafka.KafkaSource
import io.micrometer.core.instrument.MeterRegistry
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.kafka.connect.sink.SinkRecord

class DebeziumStreamletFactory(
    private val source: KafkaSource,
    private val sink: IcebergSink,
    private val tableCreator: TableCreator,
    private val streamConfig: StreamConfig,
    private val registry: MeterRegistry,
) {
    private var deduperProvider: RocksDbDeduperProvider<SinkRecord>? = null

    init {
        deduperProvider = streamConfig.dedupe?.let {
            val client = RocksDbClient.getOrCreate(it)
            RocksDbDeduperProvider(client)
        }
    }

    fun createStreamlet(topic: String, tableId: TableIdentifier): DebeziumStreamlet {
        val name = tableId.toString()

        val inlet = source.getInlet(name, topic)
        val outlet = sink.getOutlet(name, tableId, deduperProvider) ?: createOutlet(name, topic, tableId)
        val schemaHandler = KafkaSchemaSchemaHandler(outlet)

        return DebeziumStreamlet(name, inlet, outlet, schemaHandler, streamConfig, registry)
    }

    private fun createOutlet(name: String, topic: String, tableId: TableIdentifier): IcebergTableOutlet {
        tableCreator.createTable(topic, tableId)
        return sink.getOutlet(name, tableId, deduperProvider)!!
    }
}
