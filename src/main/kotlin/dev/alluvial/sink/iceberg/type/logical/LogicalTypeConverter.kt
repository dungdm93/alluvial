package dev.alluvial.sink.iceberg.type.logical

import org.apache.iceberg.orc.OrcValueReader
import org.apache.iceberg.orc.OrcValueWriter
import org.apache.iceberg.parquet.ParquetValueReader
import org.apache.iceberg.parquet.ParquetValueWriter
import java.util.function.Supplier
import org.apache.avro.Schema as AvroSchema
import org.apache.iceberg.avro.ValueReader as AvroValueReader
import org.apache.iceberg.avro.ValueWriter as AvroValueWriter
import org.apache.iceberg.types.Type as IcebergType
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.orc.TypeDescription as OrcType
import org.apache.parquet.schema.Type as ParquetType

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
