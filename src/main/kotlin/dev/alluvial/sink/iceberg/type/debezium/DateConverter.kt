package dev.alluvial.sink.iceberg.type.debezium

import dev.alluvial.sink.iceberg.parquet.KafkaParquetReaders
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
import org.apache.iceberg.parquet.ParquetValueWriters
import org.apache.iceberg.types.Types.DateType
import java.util.function.Supplier

internal object DateConverter : LogicalTypeConverter<Int, Int> {
    override val name = io.debezium.time.Date.SCHEMA_NAME

    override fun toIcebergType(idSupplier: Supplier<Int>, schema: KafkaSchema): IcebergType = DateType.get()

    override fun toIcebergValue(sValue: Int): Int = sValue

    override fun avroReader(sSchema: KafkaSchema, schema: AvroSchema): AvroValueReader<Int> =
        AvroValueReaders.ints()

    override fun avroWriter(sSchema: KafkaSchema, schema: AvroSchema): AvroValueWriter<Int> =
        AvroValueWriters.ints()

    override fun parquetReader(
        sSchema: KafkaSchema,
        type: ParquetType,
        ctx: ParquetReaderContext
    ): ParquetValueReader<Int> {
        val primitiveCtx = ctx as ParquetPrimitiveReaderContext
        return KafkaParquetReaders.unboxed(primitiveCtx.desc)
    }

    override fun parquetWriter(
        sSchema: KafkaSchema,
        type: ParquetType,
        ctx: ParquetWriterContext
    ): ParquetValueWriter<Int> {
        val primitiveCtx = ctx as ParquetPrimitiveWriterContext
        @Suppress("INACCESSIBLE_TYPE")
        return ParquetValueWriters.ints(primitiveCtx.desc)
    }

    override fun orcReader(sSchema: KafkaSchema, type: OrcType): OrcValueReader<Int> {
        TODO("Not yet implemented")
    }

    override fun orcWriter(sSchema: KafkaSchema, type: OrcType): OrcValueWriter<Int> {
        TODO("Not yet implemented")
    }
}
