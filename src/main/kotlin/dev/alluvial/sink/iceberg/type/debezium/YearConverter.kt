package dev.alluvial.sink.iceberg.type.debezium

import dev.alluvial.sink.iceberg.type.logical.LogicalTypeConverter
import dev.alluvial.sink.iceberg.type.logical.ParquetReaderContext
import dev.alluvial.sink.iceberg.type.logical.ParquetPrimitiveReaderContext
import dev.alluvial.sink.iceberg.type.logical.ParquetWriterContext
import dev.alluvial.sink.iceberg.type.logical.ParquetPrimitiveWriterContext
import org.apache.iceberg.avro.ValueReader
import org.apache.iceberg.avro.ValueReaders
import org.apache.iceberg.avro.ValueWriter
import org.apache.iceberg.avro.ValueWriters
import org.apache.iceberg.orc.OrcValueReader
import org.apache.iceberg.orc.OrcValueWriter
import org.apache.iceberg.parquet.ParquetValueReader
import org.apache.iceberg.parquet.ParquetValueReaders
import org.apache.iceberg.parquet.ParquetValueWriter
import org.apache.iceberg.parquet.ParquetValueWriters
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Types
import org.apache.kafka.connect.data.Schema
import org.apache.orc.TypeDescription
import java.util.function.Supplier

@Suppress("INACCESSIBLE_TYPE")
internal object YearConverter : LogicalTypeConverter<Int, Int> {
    override val name = io.debezium.time.Year.SCHEMA_NAME

    override fun toIcebergType(idSupplier: Supplier<Int>, schema: Schema): Type = Types.IntegerType.get()

    override fun toIcebergValue(sValue: Int): Int = sValue

    override fun avroReader(sSchema: Schema, schema: org.apache.avro.Schema): ValueReader<Int> = ValueReaders.ints()

    override fun avroWriter(sSchema: Schema, schema: org.apache.avro.Schema): ValueWriter<Int> = ValueWriters.ints()

    override fun parquetReader(
        sSchema: Schema,
        type: org.apache.parquet.schema.Type,
        ctx: ParquetReaderContext
    ): ParquetValueReader<Int> {
        val primitiveCtx = ctx as ParquetPrimitiveReaderContext
        return ParquetValueReaders.UnboxedReader(primitiveCtx.desc)
    }

    override fun parquetWriter(
        sSchema: Schema,
        type: org.apache.parquet.schema.Type,
        ctx: ParquetWriterContext
    ): ParquetValueWriter<Int> {
        val primitiveCtx = ctx as ParquetPrimitiveWriterContext
        return ParquetValueWriters.ints(primitiveCtx.desc)
    }

    override fun orcReader(sSchema: Schema, type: TypeDescription): OrcValueReader<Int> {
        TODO("Not yet implemented")
    }

    override fun orcWriter(sSchema: Schema, type: TypeDescription): OrcValueWriter<Int> {
        TODO("Not yet implemented")
    }
}
