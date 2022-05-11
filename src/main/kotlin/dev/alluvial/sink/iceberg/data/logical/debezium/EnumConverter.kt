package dev.alluvial.sink.iceberg.data.logical.debezium

import dev.alluvial.sink.iceberg.data.logical.LogicalTypeConverter
import dev.alluvial.sink.iceberg.data.logical.ParquetReaderContext
import dev.alluvial.sink.iceberg.data.logical.ParquetPrimitiveReaderContext
import dev.alluvial.sink.iceberg.data.logical.ParquetWriterContext
import dev.alluvial.sink.iceberg.data.logical.ParquetPrimitiveWriterContext
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

@Suppress("UNCHECKED_CAST")
internal object EnumConverter : LogicalTypeConverter<String, String> {
    override val name = io.debezium.data.EnumSet.LOGICAL_NAME

    override fun toIcebergType(idSupplier: Supplier<Int>, schema: Schema): Type = Types.StringType.get()

    override fun toIcebergValue(sValue: String): String = sValue

    override fun avroReader(sSchema: Schema, schema: org.apache.avro.Schema): ValueReader<String> =
        ValueReaders.strings()

    override fun avroWriter(sSchema: Schema, schema: org.apache.avro.Schema): ValueWriter<String> =
        ValueWriters.strings() as ValueWriter<String>

    override fun parquetReader(
        sSchema: Schema,
        type: org.apache.parquet.schema.Type,
        ctx: ParquetReaderContext
    ): ParquetValueReader<String> {
        val primitiveCtx = ctx as ParquetPrimitiveReaderContext
        return ParquetValueReaders.StringReader(primitiveCtx.desc)
    }

    override fun parquetWriter(
        sSchema: Schema,
        type: org.apache.parquet.schema.Type,
        ctx: ParquetWriterContext
    ): ParquetValueWriter<String> {
        val primitiveCtx = ctx as ParquetPrimitiveWriterContext
        return ParquetValueWriters.strings(primitiveCtx.desc) as ParquetValueWriter<String>
    }

    override fun orcReader(sSchema: Schema, type: TypeDescription): OrcValueReader<String> {
        TODO("Not yet implemented")
    }

    override fun orcWriter(sSchema: Schema, type: TypeDescription): OrcValueWriter<String> {
        TODO("Not yet implemented")
    }
}
