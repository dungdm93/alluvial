package dev.alluvial.schema.debezium

import dev.alluvial.sink.iceberg.type.IcebergSchema
import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.isMigrateFrom
import dev.alluvial.sink.iceberg.type.toIcebergSchema
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Table
import org.apache.iceberg.TestTables
import org.apache.kafka.connect.data.Decimal
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.SchemaBuilder.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import strikt.api.expectThat
import java.io.File

internal class TestKafkaSchemaSchemaMigrator {
    @TempDir
    private lateinit var tmpDir: File
    private lateinit var table: Table
    private val formatVersion: Int = 2

    private fun createTable(iSchema: IcebergSchema) {
        TestTables.clearTables()
        assert(tmpDir.deleteRecursively()) { "folder should be deleted" }
        table = TestTables.create(tmpDir, "test", iSchema, PartitionSpec.unpartitioned(), formatVersion)
    }

    private fun migrateAndValidate(sBefore: KafkaSchema, sAfter: KafkaSchema) {
        createTable(sBefore.toIcebergSchema())
        val iOriginal = table.schema()

        val su = table.updateSchema()
        KafkaSchemaSchemaMigrator(su).visit(sAfter, iOriginal)
        su.commit()

        table.refresh()
        val iActual = table.schema()

        createTable(sAfter.toIcebergSchema())
        val iExpect = table.schema()

        expectThat(iActual).isMigrateFrom(iOriginal, iExpect)
    }

    @Test
    fun testPrimitives() {
        val before = struct {
            field("pk", INT32_SCHEMA)
            field("dropping", STRING_SCHEMA)
            // columns will be replaced
            field("int32_to_string", INT32_SCHEMA)
            field("float32_to_boolean", OPTIONAL_FLOAT32_SCHEMA)
            field("boolean_to_float32", BOOLEAN_SCHEMA)
            field("decimal103_to_decimal107", decimal(10, 3))
            // upcasting columns
            field("int32_to_int64", INT32_SCHEMA)
            field("float32_to_float64", FLOAT32_SCHEMA)
            field("decimal103_to_decimal203", decimal(10, 3))
            // change nullability
            field("require_to_optional", STRING_SCHEMA)
            field("optional_to_require", OPTIONAL_INT32_SCHEMA)
        }
        val after = struct {
            field("pk", INT32_SCHEMA)
            field("adding", STRING_SCHEMA)
            // replaced columns
            field("int32_to_string", STRING_SCHEMA)
            field("float32_to_boolean", BOOLEAN_SCHEMA)
            field("boolean_to_float32", OPTIONAL_FLOAT32_SCHEMA)
            field("decimal103_to_decimal107", decimal(10, 7))
            // upcasted columns
            field("int32_to_int64", INT64_SCHEMA)
            field("float32_to_float64", FLOAT64_SCHEMA)
            field("decimal103_to_decimal203", decimal(20, 3))
            // change nullability & also change the column order
            field("optional_to_require", INT32_SCHEMA)
            field("require_to_optional", OPTIONAL_STRING_SCHEMA)
        }

        migrateAndValidate(before, after)
    }

    @Test
    fun testList() {
        val before = struct {
            field("pk", INT32_SCHEMA)
            // change element nullability
            field("list_require_to_optional", array(STRING_SCHEMA))
            field("list_optional_to_require", array(OPTIONAL_INT64_SCHEMA))
            // change element type
            field("list_of_primitive", array(STRING_SCHEMA))
            field("list_of_decimal", array(decimal(10, 3)))
            field("list_of_map_to_struct", array(map(INT32_SCHEMA, FLOAT32_SCHEMA)))
            field("list_of_struct", array(struct {
                field("s_unchanged", BOOLEAN_SCHEMA)
                field("s_dropped", STRING_SCHEMA)
                field("s_update_inline", decimal(10, 3))
                field("s_replace", decimal(10, 3))
            }))
        }
        val after = struct {
            field("pk", INT32_SCHEMA)
            // change element nullability
            field("list_require_to_optional", array(OPTIONAL_STRING_SCHEMA))
            field("list_optional_to_require", array(INT64_SCHEMA))
            // change element type
            field("list_of_primitive", array(INT32_SCHEMA))
            field("list_of_decimal", array(decimal(20, 3)))
            field("list_of_map_to_struct", array(struct { field("foobar", INT32_SCHEMA) }))
            field("list_of_struct", array(struct {
                field("s_unchanged", BOOLEAN_SCHEMA)
                field("s_added", FLOAT32_SCHEMA)
                field("s_update_inline", decimal(20, 3))
                field("s_replace", decimal(10, 7))
            }))
        }

        migrateAndValidate(before, after)
    }

    @Test
    fun testMap() {
        val before = struct {
            field("pk", INT32_SCHEMA)
            // change value nullability
            field("map_value_require_to_optional", map(OPTIONAL_STRING_SCHEMA, INT64_SCHEMA))
            field("map_value_optional_to_require", map(INT32_SCHEMA, OPTIONAL_BYTES_SCHEMA))
            // change key/value type
            field("map_of_primitive_key", map(STRING_SCHEMA, INT32_SCHEMA))
            field("map_of_primitive_value", map(STRING_SCHEMA, INT32_SCHEMA))
            field("map_of_decimal_key", map(decimal(10, 3), STRING_SCHEMA))
            field("map_of_decimal_value", map(STRING_SCHEMA, decimal(10, 3)))
            field("map_of_array_key", map(array(STRING_SCHEMA), STRING_SCHEMA))
            field("map_of_array_decimal_key", map(array(decimal(10, 3)), STRING_SCHEMA))
            field("map_of_array_value", map(STRING_SCHEMA, array(STRING_SCHEMA)))
            field("map_of_array_value_int_to_long", map(STRING_SCHEMA, array(INT32_SCHEMA)))
            field("map_of_array_value_decimal_upcast", map(STRING_SCHEMA, array(decimal(10, 3))))
            field("map_of_array_value_decimal_replace", map(STRING_SCHEMA, array(decimal(10, 3))))
            field("map_of_struct", map(STRING_SCHEMA, struct {
                field("s_unchanged", BOOLEAN_SCHEMA)
                field("s_dropped", STRING_SCHEMA)
                field("s_update_inline", decimal(10, 3))
                field("s_replace", decimal(10, 3))
            }))
        }
        val after = struct {
            field("pk", INT32_SCHEMA)
            // change value nullability
            field("map_value_require_to_optional", map(STRING_SCHEMA, OPTIONAL_INT64_SCHEMA))
            field("map_value_optional_to_require", map(OPTIONAL_INT32_SCHEMA, BYTES_SCHEMA))
            // change key type
            field("map_of_primitive_key", map(INT32_SCHEMA, INT32_SCHEMA))
            field("map_of_primitive_value", map(STRING_SCHEMA, BOOLEAN_SCHEMA))
            field("map_of_decimal_key", map(decimal(20, 3), STRING_SCHEMA))
            field("map_of_decimal_value", map(STRING_SCHEMA, decimal(20, 3)))
            field("map_of_array_key", map(array(INT32_SCHEMA), STRING_SCHEMA))
            field("map_of_array_decimal_key", map(array(decimal(20, 3)), STRING_SCHEMA))
            field("map_of_array_value", map(STRING_SCHEMA, array(INT32_SCHEMA)))
            field("map_of_array_value_int_to_long", map(STRING_SCHEMA, array(INT64_SCHEMA)))
            field("map_of_array_value_decimal_upcast", map(STRING_SCHEMA, array(decimal(20, 3))))
            field("map_of_array_value_decimal_replace", map(STRING_SCHEMA, array(decimal(10, 7))))
            field("map_of_struct", map(STRING_SCHEMA, struct {
                field("s_unchanged", BOOLEAN_SCHEMA)
                field("s_added", FLOAT32_SCHEMA)
                field("s_update_inline", decimal(20, 3))
                field("s_replace", decimal(10, 7))
            }))
        }

        migrateAndValidate(before, after)
    }

    @Test
    fun testStruct() {
        val before = struct {
            field("pk", INT32_SCHEMA)
            field("struct", struct {
                // field of primitive
                field("f_string_to_int32", STRING_SCHEMA)
                field("f_required_to_optional", STRING_SCHEMA)
                field("f_optional_to_required", OPTIONAL_STRING_SCHEMA)
                field("f_decimal103_to_decimal203", decimal(10, 3))
                field("f_decimal103_to_decimal107", decimal(10, 3))
                // list
                field("f_list_of_primitive", array(STRING_SCHEMA))
                field("f_list_of_decimal", array(decimal(10, 3)))
                field("f_list_of_map_to_struct", array(map(INT32_SCHEMA, FLOAT32_SCHEMA)))
                field("f_list_of_struct", array(struct {
                    field("s_unchanged", BOOLEAN_SCHEMA)
                    field("s_dropped", STRING_SCHEMA)
                    field("s_update_inline", decimal(10, 3))
                    field("s_replace", decimal(10, 3))
                }))
                // map
                field("f_map_of_primitive_key", map(STRING_SCHEMA, INT32_SCHEMA))
                field("f_map_of_primitive_value", map(STRING_SCHEMA, INT32_SCHEMA))
                field("f_map_of_decimal_key", map(decimal(10, 3), STRING_SCHEMA))
                field("f_map_of_decimal_value", map(STRING_SCHEMA, decimal(10, 3)))
                field("f_map_of_array_key", map(array(STRING_SCHEMA), STRING_SCHEMA))
                field("f_map_of_array_decimal_key", map(array(decimal(10, 3)), STRING_SCHEMA))
                field("f_map_of_array_value", map(STRING_SCHEMA, array(STRING_SCHEMA)))
                field("f_map_of_array_value_int_to_long", map(STRING_SCHEMA, array(INT32_SCHEMA)))
                field("f_map_of_array_value_decimal_upcast", map(STRING_SCHEMA, array(decimal(10, 3))))
                field("f_map_of_array_value_decimal_replace", map(STRING_SCHEMA, array(decimal(10, 3))))
                field("f_map_of_struct", map(STRING_SCHEMA, struct {
                    field("s_unchanged", BOOLEAN_SCHEMA)
                    field("s_dropped", STRING_SCHEMA)
                    field("s_update_inline", decimal(10, 3))
                    field("s_replace", decimal(10, 3))
                }))
            })
        }
        val after = struct {
            field("pk", INT32_SCHEMA)
            field("struct", struct {
                // field of primitive
                field("f_string_to_int32", INT32_SCHEMA)
                field("f_optional_to_required", STRING_SCHEMA)
                field("f_required_to_optional", OPTIONAL_STRING_SCHEMA)
                field("f_decimal103_to_decimal203", decimal(20, 3))
                field("f_decimal103_to_decimal107", decimal(10, 7))
                // list
                field("f_list_of_primitive", array(INT32_SCHEMA))
                field("f_list_of_decimal", array(decimal(20, 3)))
                field("f_list_of_map_to_struct", array(struct { field("foobar", INT32_SCHEMA) }))
                field("f_list_of_struct", array(struct {
                    field("s_unchanged", BOOLEAN_SCHEMA)
                    field("s_added", FLOAT32_SCHEMA)
                    field("s_update_inline", decimal(20, 3))
                    field("s_replace", decimal(10, 7))
                }))
                // map
                field("f_map_of_primitive_key", map(INT32_SCHEMA, INT32_SCHEMA))
                field("f_map_of_primitive_value", map(STRING_SCHEMA, BOOLEAN_SCHEMA))
                field("f_map_of_decimal_key", map(decimal(20, 3), STRING_SCHEMA))
                field("f_map_of_decimal_value", map(STRING_SCHEMA, decimal(20, 3)))
                field("f_map_of_array_key", map(array(INT32_SCHEMA), STRING_SCHEMA))
                field("f_map_of_array_decimal_key", map(array(decimal(20, 3)), STRING_SCHEMA))
                field("f_map_of_array_value", map(STRING_SCHEMA, array(INT32_SCHEMA)))
                field("f_map_of_array_value_int_to_long", map(STRING_SCHEMA, array(INT64_SCHEMA)))
                field("f_map_of_array_value_decimal_upcast", map(STRING_SCHEMA, array(decimal(20, 3))))
                field("f_map_of_array_value_decimal_replace", map(STRING_SCHEMA, array(decimal(10, 7))))
                field("f_map_of_struct", map(STRING_SCHEMA, struct {
                    field("s_unchanged", BOOLEAN_SCHEMA)
                    field("s_added", FLOAT32_SCHEMA)
                    field("s_update_inline", decimal(20, 3))
                    field("s_replace", decimal(10, 7))
                }))
            })
        }

        migrateAndValidate(before, after)
    }

    private fun decimal(precision: Int, scale: Int): KafkaSchema {
        return Decimal.builder(scale).parameter("precision", precision.toString()).build()
    }

    private inline fun struct(block: SchemaBuilder.() -> Unit): KafkaSchema {
        return struct().also(block).build()
    }
}
