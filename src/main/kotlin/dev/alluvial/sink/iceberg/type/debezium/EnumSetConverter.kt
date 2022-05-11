package dev.alluvial.sink.iceberg.type.debezium

import dev.alluvial.sink.iceberg.type.logical.LogicalTypeConverter
import dev.alluvial.sink.iceberg.type.logical.ParquetReaderContext
import dev.alluvial.sink.iceberg.type.logical.ParquetWriterContext
import org.apache.avro.io.Decoder
import org.apache.avro.io.Encoder
import org.apache.iceberg.avro.ValueReader
import org.apache.iceberg.avro.ValueReaders
import org.apache.iceberg.avro.ValueWriter
import org.apache.iceberg.avro.ValueWriters
import org.apache.iceberg.orc.OrcValueReader
import org.apache.iceberg.orc.OrcValueWriter
import org.apache.iceberg.parquet.ParquetValueReader
import org.apache.iceberg.parquet.ParquetValueWriter
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Types
import org.apache.kafka.connect.data.Schema
import org.apache.orc.TypeDescription
import java.util.function.Supplier

internal object EnumSetConverter : LogicalTypeConverter<String, List<String>> {
    override val name = io.debezium.data.EnumSet.LOGICAL_NAME

    private object AvroReader : ValueReader<String> {
        private val delegatedReader = ValueReaders.array(ValueReaders.strings())

        override fun read(decoder: Decoder, reuse: Any?): String {
            val list = delegatedReader.read(decoder, reuse)
            return list.joinToString(",")
        }
    }

    private object AvroWriter : ValueWriter<String> {
        private val delegatedWriter = ValueWriters.array(ValueWriters.strings())

        override fun write(enumStr: String, encoder: Encoder) {
            val enumSet = if (enumStr.isEmpty())
                emptyList() else
                enumStr.split(",")

            delegatedWriter.write(enumSet, encoder)
        }
    }

    override fun toIcebergType(idSupplier: Supplier<Int>, schema: Schema): Type =
        Types.ListType.ofRequired(idSupplier.get(), Types.StringType.get())

    override fun toIcebergValue(sValue: String): List<String> {
        return if (sValue.isEmpty())
            emptyList() else
            sValue.split(",")
    }

    override fun avroReader(sSchema: Schema, schema: org.apache.avro.Schema): ValueReader<String> = AvroReader

    override fun avroWriter(sSchema: Schema, schema: org.apache.avro.Schema): ValueWriter<String> = AvroWriter

    override fun parquetReader(
        sSchema: Schema,
        type: org.apache.parquet.schema.Type,
        ctx: ParquetReaderContext
    ): ParquetValueReader<String> {
        TODO("Not yet implemented")
    }

    override fun parquetWriter(
        sSchema: Schema,
        type: org.apache.parquet.schema.Type,
        ctx: ParquetWriterContext
    ): ParquetValueWriter<String> {
        TODO("Not yet implemented")
    }

    override fun orcReader(sSchema: Schema, type: TypeDescription): OrcValueReader<String> {
        TODO("Not yet implemented")
    }

    override fun orcWriter(sSchema: Schema, type: TypeDescription): OrcValueWriter<String> {
        TODO("Not yet implemented")
    }
}
