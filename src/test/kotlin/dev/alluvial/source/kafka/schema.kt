@file:Suppress("HasPlatformType")

package dev.alluvial.source.kafka

import org.apache.kafka.connect.data.Schema.*

val KAFKA_PRIMITIVES_SCHEMA = mapOf(
    "int8" to INT8_SCHEMA,
    "int16" to INT16_SCHEMA,
    "int32" to INT32_SCHEMA,
    "int64" to INT64_SCHEMA,
    "float32" to FLOAT32_SCHEMA,
    "float64" to FLOAT64_SCHEMA,
    "boolean" to BOOLEAN_SCHEMA,
    "string" to STRING_SCHEMA,
    "bytes" to BYTES_SCHEMA,
    "int8_o" to OPTIONAL_INT8_SCHEMA,
    "int16_o" to OPTIONAL_INT16_SCHEMA,
    "int32_o" to OPTIONAL_INT32_SCHEMA,
    "int64_o" to OPTIONAL_INT64_SCHEMA,
    "float32_o" to OPTIONAL_FLOAT32_SCHEMA,
    "float64_o" to OPTIONAL_FLOAT64_SCHEMA,
    "boolean_o" to OPTIONAL_BOOLEAN_SCHEMA,
    "string_o" to OPTIONAL_STRING_SCHEMA,
    "bytes_o" to OPTIONAL_BYTES_SCHEMA,
)

val DECIMAL_19_3_SCHEMA = org.apache.kafka.connect.data.Decimal.builder(3)
    .parameter("connect.decimal.precision", "19")
    .build()
val DECIMAL_38_0_SCHEMA = org.apache.kafka.connect.data.Decimal.builder(0)
    .parameter("connect.decimal.precision", "38")
    .build()
val DECIMAL_38_10_SCHEMA = org.apache.kafka.connect.data.Decimal.builder(10)
    .parameter("connect.decimal.precision", "38")
    .build()
val DECIMAL_E_10_SCHEMA = org.apache.kafka.connect.data.Decimal.builder(10).build()
val DATE_SCHEMA = org.apache.kafka.connect.data.Date.SCHEMA
val TIME_SCHEMA = org.apache.kafka.connect.data.Time.SCHEMA
val TIMESTAMP_SCHEMA = org.apache.kafka.connect.data.Timestamp.SCHEMA
val KAFKA_LOGICAL_TYPES_SCHEMA = mapOf(
    "decimal_19_3" to DECIMAL_19_3_SCHEMA,
    "decimal_38_0" to DECIMAL_38_0_SCHEMA,
    "decimal_38_10" to DECIMAL_38_10_SCHEMA,
    "decimal_e_10" to DECIMAL_E_10_SCHEMA,
    "date" to DATE_SCHEMA,
    "time" to TIME_SCHEMA,
    "timestamp" to TIMESTAMP_SCHEMA,
)

val DBZ_DATE_SCHEMA = io.debezium.time.Date.schema()
val DBZ_MILLI_TIME_SCHEMA = io.debezium.time.Time.schema()
val DBZ_MICRO_TIME_SCHEMA = io.debezium.time.MicroTime.schema()
val DBZ_NANO_TIME_SCHEMA = io.debezium.time.NanoTime.schema()
val DBZ_ZONED_TIME_SCHEMA = io.debezium.time.ZonedTime.schema()
val DBZ_MILLI_TIMESTAMP_SCHEMA = io.debezium.time.Timestamp.schema()
val DBZ_MICRO_TIMESTAMP_SCHEMA = io.debezium.time.MicroTimestamp.schema()
val DBZ_NANO_TIMESTAMP_SCHEMA = io.debezium.time.NanoTimestamp.schema()
val DBZ_ZONED_TIMESTAMP_SCHEMA = io.debezium.time.ZonedTimestamp.schema()
//val DBZ_MICRO_DURATION_SCHEMA = io.debezium.time.MicroDuration.schema()
//val DBZ_NANO_DURATION_SCHEMA = io.debezium.time.NanoDuration.schema()
//val DBZ_INTERVAL_SCHEMA = io.debezium.time.Interval.schema()
//val DBZ_YEAR_SCHEMA = io.debezium.time.Year.schema()
//val DBZ_BITS_1_SCHEMA = io.debezium.data.Bits.schema(1)
//val DBZ_BITS_10_SCHEMA = io.debezium.data.Bits.schema(10)
//val DBZ_JSON_SCHEMA = io.debezium.data.Json.schema()
//val DBZ_XML_SCHEMA = io.debezium.data.Xml.schema()
//val DBZ_UUID_SCHEMA = io.debezium.data.Uuid.schema()
//val DBZ_LTREE_SCHEMA = io.debezium.data.Ltree.schema()
//val DBZ_ENUM_SCHEMA = io.debezium.data.Enum.schema(listOf("first", "second", "third", "fourth", "fifth"))
//val DBZ_ENUM_SET_SCHEMA = io.debezium.data.EnumSet.schema(listOf("first", "second", "third", "fourth", "fifth"))
//val DBZ_VAR_DECIMAL_SCHEMA = io.debezium.data.VariableScaleDecimal.schema()
//val DBZ_POINT_SCHEMA = io.debezium.data.geometry.Point.schema()
//val DBZ_GEOMETRY_SCHEMA = io.debezium.data.geometry.Geometry.schema()
//val DBZ_GEOGRAPHY_SCHEMA = io.debezium.data.geometry.Geography.schema()

val DEBEZIUM_LOGICAL_TYPES_SCHEMA = mapOf(
    "dbz_date" to DBZ_DATE_SCHEMA,
    "dbz_milli_time" to DBZ_MILLI_TIME_SCHEMA,
    "dbz_micro_time" to DBZ_MICRO_TIME_SCHEMA,
    "dbz_nano_time" to DBZ_NANO_TIME_SCHEMA,
    "dbz_zoned_time" to DBZ_ZONED_TIME_SCHEMA,
    "dbz_milli_timestamp" to DBZ_MILLI_TIMESTAMP_SCHEMA,
    "dbz_micro_timestamp" to DBZ_MICRO_TIMESTAMP_SCHEMA,
    "dbz_nano_timestamp" to DBZ_NANO_TIMESTAMP_SCHEMA,
    "dbz_zoned_timestamp" to DBZ_ZONED_TIMESTAMP_SCHEMA,
//    "dbz_micro_duration" to DBZ_MICRO_DURATION_SCHEMA,
//    "dbz_nano_duration" to DBZ_NANO_DURATION_SCHEMA,
//    "dbz_interval" to DBZ_INTERVAL_SCHEMA,
//    "dbz_year" to DBZ_YEAR_SCHEMA,
//    "dbz_bits_1" to DBZ_BITS_1_SCHEMA,
//    "dbz_bits_10" to DBZ_BITS_10_SCHEMA,
//    "dbz_json" to DBZ_JSON_SCHEMA,
//    "dbz_xml" to DBZ_XML_SCHEMA,
//    "dbz_uuid" to DBZ_UUID_SCHEMA,
//    "dbz_ltree" to DBZ_LTREE_SCHEMA,
//    "dbz_enum" to DBZ_ENUM_SCHEMA,
//    "dbz_enum_set" to DBZ_ENUM_SET_SCHEMA,
//    "dbz_var_decimal" to DBZ_VAR_DECIMAL_SCHEMA,
//    "dbz_point" to DBZ_POINT_SCHEMA,
//    "dbz_geometry" to DBZ_GEOMETRY_SCHEMA,
//    "dbz_geography" to DBZ_GEOGRAPHY_SCHEMA,
)
