package dev.alluvial.sink.iceberg.type.debezium

import dev.alluvial.sink.iceberg.type.AvroSchema
import dev.alluvial.sink.iceberg.type.AvroValueReader
import dev.alluvial.sink.iceberg.type.AvroValueReaders
import dev.alluvial.sink.iceberg.type.AvroValueWriter
import dev.alluvial.sink.iceberg.type.AvroValueWriters
import dev.alluvial.sink.iceberg.type.IcebergType
import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.OrcType
import dev.alluvial.sink.iceberg.type.OrcValueReader
import dev.alluvial.sink.iceberg.type.OrcValueWriter
import dev.alluvial.sink.iceberg.type.ParquetType
import dev.alluvial.sink.iceberg.type.ParquetValueReader
import dev.alluvial.sink.iceberg.type.ParquetValueWriter
import dev.alluvial.sink.iceberg.type.logical.LogicalTypeConverter
import dev.alluvial.sink.iceberg.type.logical.ParquetPrimitiveReaderContext
import dev.alluvial.sink.iceberg.type.logical.ParquetPrimitiveWriterContext
import dev.alluvial.sink.iceberg.type.logical.ParquetReaderContext
import dev.alluvial.sink.iceberg.type.logical.ParquetWriterContext
import org.apache.iceberg.parquet.ParquetValueReaders
import org.apache.iceberg.parquet.ParquetValueWriters
import org.apache.iceberg.types.Types.StringType
import java.util.function.Supplier

@Suppress("UNCHECKED_CAST")
internal object EnumConverter : LogicalTypeConverter<String, String> {
    override val name = io.debezium.data.EnumSet.LOGICAL_NAME

    override fun toIcebergType(idSupplier: Supplier<Int>, schema: KafkaSchema): IcebergType = StringType.get()

    override fun toIcebergValue(sValue: String): String = sValue

    override fun avroReader(sSchema: KafkaSchema, schema: AvroSchema): AvroValueReader<String> =
        AvroValueReaders.strings()

    override fun avroWriter(sSchema: KafkaSchema, schema: AvroSchema): AvroValueWriter<String> =
        AvroValueWriters.strings() as AvroValueWriter<String>

    override fun parquetReader(
        sSchema: KafkaSchema,
        type: ParquetType,
        ctx: ParquetReaderContext
    ): ParquetValueReader<String> {
        val primitiveCtx = ctx as ParquetPrimitiveReaderContext
        return ParquetValueReaders.StringReader(primitiveCtx.desc)
    }

    override fun parquetWriter(
        sSchema: KafkaSchema,
        type: ParquetType,
        ctx: ParquetWriterContext
    ): ParquetValueWriter<String> {
        val primitiveCtx = ctx as ParquetPrimitiveWriterContext
        return ParquetValueWriters.strings(primitiveCtx.desc) as ParquetValueWriter<String>
    }

    override fun orcReader(sSchema: KafkaSchema, type: OrcType): OrcValueReader<String> {
        TODO("Not yet implemented")
    }

    override fun orcWriter(sSchema: KafkaSchema, type: OrcType): OrcValueWriter<String> {
        TODO("Not yet implemented")
    }
}
