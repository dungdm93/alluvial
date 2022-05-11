package dev.alluvial.sink.iceberg.data.logical

import org.apache.iceberg.parquet.ParquetValueReader
import org.apache.parquet.column.ColumnDescriptor

interface ParquetReaderContext {
    companion object {
        fun struct(fields: List<ParquetValueReader<*>>): ParquetReaderContext {
            return ParquetStructReaderContext(fields)
        }

        fun list(element: ParquetValueReader<*>): ParquetReaderContext {
            return ParquetListReaderContext(element)
        }

        fun map(key: ParquetValueReader<*>, value: ParquetValueReader<*>): ParquetReaderContext {
            return ParquetMapReaderContext(key, value)
        }

        fun primitive(desc: ColumnDescriptor): ParquetReaderContext {
            return ParquetPrimitiveReaderContext(desc)
        }
    }
}

internal data class ParquetStructReaderContext(
    val fields: List<ParquetValueReader<*>>
) : ParquetReaderContext

internal data class ParquetListReaderContext(
    val element: ParquetValueReader<*>
) : ParquetReaderContext

internal data class ParquetMapReaderContext(
    val key: ParquetValueReader<*>,
    val value: ParquetValueReader<*>
) : ParquetReaderContext

internal data class ParquetPrimitiveReaderContext(
    val desc: ColumnDescriptor
) : ParquetReaderContext
