package dev.alluvial.sink.iceberg.parquet

import dev.alluvial.sink.iceberg.type.IcebergSchema
import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.toIcebergSchema
import dev.alluvial.sink.iceberg.type.toKafkaSchema
import org.apache.kafka.connect.data.ConnectSchema

@Suppress("UnnecessaryVariable")
internal class KafkaParquetRWReorderFieldsTest : KafkaParquetRWTest() {
    override fun convert(iSchema: IcebergSchema): KafkaSchema {
        val sSchema = iSchema.toKafkaSchema().shuffleFields()
        return sSchema
    }

    override fun convert(sSchema: KafkaSchema): IcebergSchema {
        val iSchema = sSchema.shuffleFields().toIcebergSchema()
        return iSchema
    }

    private fun KafkaSchema.shuffleFields(): KafkaSchema {
        val fields = this.fields().shuffled()
        return ConnectSchema(
            this.type(),
            this.isOptional,
            this.defaultValue(),
            this.name(),
            this.version(),
            this.doc(),
            this.parameters(),
            fields,
            null,
            null,
        )
    }
}
