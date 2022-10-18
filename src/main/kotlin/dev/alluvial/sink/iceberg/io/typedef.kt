package dev.alluvial.sink.iceberg.io

import org.apache.iceberg.StructLike
import org.apache.kafka.connect.data.Struct

typealias Key = StructLike
typealias Partition = StructLike
typealias Getter<T> = (Struct) -> T
typealias Keyer<T> = (T) -> Key?
typealias Partitioner<T> = (T) -> Partition?
