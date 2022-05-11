package dev.alluvial.sink.iceberg.type.logical

import dev.alluvial.sink.iceberg.type.AvroSchema
import dev.alluvial.sink.iceberg.type.AvroValueReader
import dev.alluvial.sink.iceberg.type.AvroValueWriter
import dev.alluvial.sink.iceberg.type.IcebergType
import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.OrcType
import dev.alluvial.sink.iceberg.type.OrcValueReader
import dev.alluvial.sink.iceberg.type.OrcValueWriter
import dev.alluvial.sink.iceberg.type.ParquetType
import dev.alluvial.sink.iceberg.type.ParquetValueReader
import dev.alluvial.sink.iceberg.type.ParquetValueWriter
import java.util.function.Supplier

interface LogicalTypeConverter<K, I> {
    val name: String

    fun toIcebergType(idSupplier: Supplier<Int>, schema: KafkaSchema): IcebergType
    fun toIcebergValue(sValue: K): I

    fun avroReader(sSchema: KafkaSchema, schema: AvroSchema): AvroValueReader<K>
    fun avroWriter(sSchema: KafkaSchema, schema: AvroSchema): AvroValueWriter<K>

    fun parquetReader(sSchema: KafkaSchema, type: ParquetType, ctx: ParquetReaderContext): ParquetValueReader<K>
    fun parquetWriter(sSchema: KafkaSchema, type: ParquetType, ctx: ParquetWriterContext): ParquetValueWriter<K>

    fun orcReader(sSchema: KafkaSchema, type: OrcType): OrcValueReader<K>
    fun orcWriter(sSchema: KafkaSchema, type: OrcType): OrcValueWriter<K>
}
