package dev.alluvial.sink.iceberg.parquet

import org.apache.iceberg.avro.AvroSchemaUtil
import org.apache.iceberg.relocated.com.google.common.base.Preconditions
import org.apache.iceberg.relocated.com.google.common.collect.Lists
import org.apache.iceberg.util.Pair
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.MapLogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Type.Repetition
import java.util.Deque

/**
 * An abstract parquet schema visitor with partner type. The visitor rely on the structure matching exactly
 * and are guaranteed that because both schemas are derived from the same Iceberg schema.
 *
 * @param <P> Partner type.
 * @param <T> Return T.
 */
abstract class ParquetWithPartnerByStructureVisitor<P, T> {
    private val fieldNames: Deque<String> = Lists.newLinkedList()

    fun visit(sType: P, type: Type): T {
        return when (type) {
            is GroupType -> {
                when (type.logicalTypeAnnotation) {
                    is ListLogicalTypeAnnotation -> visitList(sType, type)
                    is MapLogicalTypeAnnotation -> visitMap(sType, type)
                    else -> visitStruct(sType, type)
                }
            }
            is PrimitiveType -> primitive(sType, type)
            else -> throw IllegalArgumentException("Unknown parquet type $type")
        }
    }

    private inline fun <PT : Type> withNode(type: PT, block: (PT) -> T): T {
        try {
            fieldNames.push(type.name)
            return block(type)
        } finally {
            fieldNames.pop()
        }
    }

    private fun visitList(sList: P, list: GroupType): T {
        Preconditions.checkArgument(
            !list.isRepetition(Repetition.REPEATED),
            "Invalid list: top-level group is repeated: %s", list
        )
        Preconditions.checkArgument(
            list.fieldCount == 1,
            "Invalid list: does not contain single repeated field: %s", list
        )

        val repeatedElement = list.getType(0).asGroupType()
        Preconditions.checkArgument(
            repeatedElement.isRepetition(Repetition.REPEATED),
            "Invalid list: inner group is not repeated"
        )
        Preconditions.checkArgument(
            repeatedElement.fieldCount <= 1,
            "Invalid list: repeated group is not a single field: %s", list
        )

        Preconditions.checkArgument(
            isArrayType(sList),
            "Invalid list: %s is not an array", sList
        )
        val sElement = arrayElementType(sList)

        return withNode(repeatedElement) {
            if (it.fieldCount == 0) {
                list(sList, list, null)
            }

            val elementResult = withNode(it.getType(0)) { element ->
                visit(sElement, element)
            }
            list(sList, list, elementResult)
        }
    }

    private fun visitMap(sMap: P, map: GroupType): T {
        Preconditions.checkArgument(
            !map.isRepetition(Repetition.REPEATED),
            "Invalid map: top-level group is repeated: %s", map
        )
        Preconditions.checkArgument(
            map.fieldCount == 1,
            "Invalid map: does not contain single repeated field: %s", map
        )

        val repeatedKeyValue = map.getType(0).asGroupType()
        Preconditions.checkArgument(
            repeatedKeyValue.isRepetition(Repetition.REPEATED),
            "Invalid map: inner group is not repeated"
        )
        Preconditions.checkArgument(
            repeatedKeyValue.fieldCount <= 2,
            "Invalid map: repeated group does not have 2 fields: %s", map
        )

        Preconditions.checkArgument(
            isMapType(sMap),
            "Invalid map: %s is not a map", sMap
        )
        val sKeyType = mapKeyType(sMap)
        val sValueType = mapValueType(sMap)

        return withNode(repeatedKeyValue) {
            var keyResult: T? = null
            var valueResult: T? = null
            when (it.fieldCount) {
                2 -> {
                    // if there are 2 fields, both key and value are projected
                    keyResult = withNode(repeatedKeyValue.getType(0)) { keyType ->
                        visit(sKeyType, keyType)
                    }
                    valueResult = withNode(repeatedKeyValue.getType(1)) { valueType ->
                        visit(sValueType, valueType)
                    }
                }
                1 -> {
                    // if there is just one, use the name to determine what it is
                    val keyOrValue = repeatedKeyValue.getType(0)
                    if (keyOrValue.name.equals("key", ignoreCase = true)) {
                        keyResult = withNode(keyOrValue) { keyType ->
                            visit(sKeyType, keyType)
                        }
                        // valueResult remains null
                    } else {
                        valueResult = withNode(keyOrValue) { valueType ->
                            visit(sValueType, valueType)
                        }
                        // keyResult remains null
                    }
                }
            }
            return map(sMap, map, keyResult, valueResult)
        }
    }

    private fun visitStruct(sStruct: P, struct: GroupType): T {
        Preconditions.checkArgument(
            isStructType(sStruct),
            "Invalid struct: %s is not a struct", sStruct
        )
        val fieldResults = struct.fields.mapIndexed { idx, field ->
            val sNameAndType = fieldNameAndType(sStruct, idx)
            Preconditions.checkArgument(
                field.name == AvroSchemaUtil.makeCompatibleName(sNameAndType.first()),
                "Structs do not match: field %s != %s", field.name, sNameAndType.first()
            )
            withNode(field) {
                visit(sNameAndType.second(), field)
            }
        }
        return struct(sStruct, struct, fieldResults)
    }

    protected fun currentPath(): Array<String> {
        return fieldNames.reversed().toTypedArray()
    }

    protected fun path(name: String): Array<String> {
        val list = fieldNames.toMutableList().asReversed()
        list.add(name)
        return list.toTypedArray()
    }

    // ---------------------------------- Partner type methods ---------------------------------------------

    protected abstract fun isArrayType(type: P): Boolean
    protected abstract fun arrayElementType(arrayType: P): P

    protected abstract fun isMapType(type: P): Boolean
    protected abstract fun mapKeyType(mapType: P): P
    protected abstract fun mapValueType(mapType: P): P

    protected abstract fun isStructType(type: P): Boolean
    protected abstract fun fieldNameAndType(structType: P, pos: Int): Pair<String, P>

    protected abstract fun nullType(): P?

    // ---------------------------------- Type visitors ---------------------------------------------

    abstract fun struct(sStruct: P, struct: GroupType, fields: List<T?>): T

    abstract fun list(sArray: P, array: GroupType, element: T?): T

    abstract fun map(sMap: P, map: GroupType, key: T?, value: T?): T

    abstract fun primitive(sPrimitive: P, primitive: PrimitiveType): T
}
