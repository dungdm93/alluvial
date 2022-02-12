package dev.alluvial.sink.iceberg.io

import org.apache.iceberg.StructLike
import org.apache.kafka.connect.data.Struct

typealias Getter = (Struct) -> Any?
typealias Keyer<T> = (T) -> StructLike?
typealias Partitioner<T> = (T) -> StructLike?
