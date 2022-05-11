package dev.alluvial.sink.iceberg.type

typealias IcebergTable = org.apache.iceberg.Table
typealias IcebergSchema = org.apache.iceberg.Schema
typealias IcebergType = org.apache.iceberg.types.Type
typealias IcebergTypeID = org.apache.iceberg.types.Type.TypeID
typealias IcebergField = org.apache.iceberg.types.Types.NestedField
typealias IcebergRecord = org.apache.iceberg.data.Record

typealias KafkaSchema = org.apache.kafka.connect.data.Schema
typealias KafkaType = org.apache.kafka.connect.data.Schema.Type
typealias KafkaField = org.apache.kafka.connect.data.Field
typealias KafkaStruct = org.apache.kafka.connect.data.Struct

typealias AvroSchema = org.apache.avro.Schema
typealias AvroType = org.apache.avro.Schema.Type
typealias AvroValueReader<T> = org.apache.iceberg.avro.ValueReader<T>
typealias AvroValueWriter<T> = org.apache.iceberg.avro.ValueWriter<T>
typealias AvroValueReaders = org.apache.iceberg.avro.ValueReaders
typealias AvroValueWriters = org.apache.iceberg.avro.ValueWriters

typealias OrcType = org.apache.orc.TypeDescription
typealias OrcValueReader<T> = org.apache.iceberg.orc.OrcValueReader<T>
// org.apache.iceberg.data.orc.GenericOrcReaders
typealias OrcValueWriter<T> = org.apache.iceberg.orc.OrcValueWriter<T>
// org.apache.iceberg.data.orc.GenericOrcWriters

typealias ParquetType = org.apache.parquet.schema.Type
typealias ParquetValueReader<T> = org.apache.iceberg.parquet.ParquetValueReader<T>
// org.apache.iceberg.parquet.ParquetValueReaders
typealias ParquetValueWriter<T> = org.apache.iceberg.parquet.ParquetValueWriter<T>
// org.apache.iceberg.parquet.ParquetValueWriters
