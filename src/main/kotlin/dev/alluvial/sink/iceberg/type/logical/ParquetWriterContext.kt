package dev.alluvial.sink.iceberg.type.logical

import org.apache.iceberg.parquet.ParquetValueWriter
import org.apache.parquet.column.ColumnDescriptor

interface ParquetWriterContext {
    companion object {
        fun struct(fields: List<ParquetValueWriter<*>>): ParquetWriterContext {
            return ParquetStructWriterContext(fields)
        }

        fun list(element: ParquetValueWriter<*>): ParquetWriterContext {
            return ParquetListWriterContext(element)
        }

        fun map(key: ParquetValueWriter<*>, value: ParquetValueWriter<*>): ParquetWriterContext {
            return ParquetMapWriterContext(key, value)
        }

        fun primitive(desc: ColumnDescriptor): ParquetWriterContext {
            return ParquetPrimitiveWriterContext(desc)
        }
    }
}

internal data class ParquetStructWriterContext(
    val fields: List<ParquetValueWriter<*>>
) : ParquetWriterContext

internal data class ParquetListWriterContext(
    val element: ParquetValueWriter<*>
) : ParquetWriterContext

internal data class ParquetMapWriterContext(
    val key: ParquetValueWriter<*>,
    val value: ParquetValueWriter<*>
) : ParquetWriterContext

internal data class ParquetPrimitiveWriterContext(
    val desc: ColumnDescriptor
) : ParquetWriterContext
