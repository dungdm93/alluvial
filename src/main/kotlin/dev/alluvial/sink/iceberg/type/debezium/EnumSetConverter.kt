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
import dev.alluvial.sink.iceberg.type.logical.ParquetReaderContext
import dev.alluvial.sink.iceberg.type.logical.ParquetWriterContext
import org.apache.avro.io.Decoder
import org.apache.avro.io.Encoder
import org.apache.iceberg.types.Types.ListType
import org.apache.iceberg.types.Types.StringType
import java.util.function.Supplier

internal object EnumSetConverter : LogicalTypeConverter<String, List<String>> {
    override val name = io.debezium.data.EnumSet.LOGICAL_NAME

    private object AvroReader : AvroValueReader<String> {
        private val delegatedReader = AvroValueReaders.array(AvroValueReaders.strings())

        override fun read(decoder: Decoder, reuse: Any?): String {
            val list = delegatedReader.read(decoder, reuse)
            return list.joinToString(",")
        }
    }

    private object AvroWriter : AvroValueWriter<String> {
        private val delegatedWriter = AvroValueWriters.array(AvroValueWriters.strings())

        override fun write(enumStr: String, encoder: Encoder) {
            val enumSet = if (enumStr.isEmpty())
                emptyList() else
                enumStr.split(",")

            delegatedWriter.write(enumSet, encoder)
        }
    }

    override fun toIcebergType(idSupplier: Supplier<Int>, schema: KafkaSchema): IcebergType =
        ListType.ofRequired(idSupplier.get(), StringType.get())

    override fun toIcebergValue(sValue: String): List<String> {
        return if (sValue.isEmpty())
            emptyList() else
            sValue.split(",")
    }

    override fun avroReader(sSchema: KafkaSchema, schema: AvroSchema): AvroValueReader<String> =
        AvroReader

    override fun avroWriter(sSchema: KafkaSchema, schema: AvroSchema): AvroValueWriter<String> =
        AvroWriter

    override fun parquetReader(
        sSchema: KafkaSchema,
        type: ParquetType,
        ctx: ParquetReaderContext
    ): ParquetValueReader<String> {
        TODO("Not yet implemented")
    }

    override fun parquetWriter(
        sSchema: KafkaSchema,
        type: ParquetType,
        ctx: ParquetWriterContext
    ): ParquetValueWriter<String> {
        TODO("Not yet implemented")
    }

    override fun orcReader(sSchema: KafkaSchema, type: OrcType): OrcValueReader<String> {
        TODO("Not yet implemented")
    }

    override fun orcWriter(sSchema: KafkaSchema, type: OrcType): OrcValueWriter<String> {
        TODO("Not yet implemented")
    }
}
