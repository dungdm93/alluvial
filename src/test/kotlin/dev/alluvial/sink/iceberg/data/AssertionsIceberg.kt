package dev.alluvial.sink.iceberg.data

import org.apache.iceberg.Schema
import org.apache.iceberg.relocated.com.google.common.base.Joiner
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Type.PrimitiveType
import org.apache.iceberg.types.Type.TypeID.*
import org.apache.iceberg.types.TypeUtil
import org.apache.iceberg.types.Types.*
import strikt.api.Assertion
import strikt.assertions.isEqualTo
import java.util.Deque
import java.util.LinkedList

fun Assertion.Builder<Schema>.isMigrateFrom(original: Schema, expected: Schema) =
    compose("is migrate to %s", expected) { actual ->
        SchemaMigrateValidation(this).visit(expected, actual, original)
    } then {
        if (anyFailed) fail() else pass()
    }

/**
 * Assert 2 types are the same, ignore `fieldId`
 */
fun <T : Type> Assertion.Builder<T>.isSameTypeAs(expected: Type): Assertion.Builder<T> =
    when {
        subject.typeId() != expected.typeId() -> assert("is the same type as %s", expected) { actual ->
            fail(actual)
        }
        subject.isPrimitiveType -> assert("is the same type as %s", expected) { actual ->
            if (actual != expected) fail(actual) else pass(actual)
        }
        else -> compose("is the same type as %s", expected) { actual ->
            when (actual.typeId()) {
                LIST -> {
                    val aListFields = actual.asListType().fields()
                    val eListFields = expected.asListType().fields()
                    get("list element [%s]") { aListFields.first() }.isSameFieldAs(eListFields.first())
                }
                MAP -> {
                    val aMapFields = actual.asMapType().fields()
                    val eMapFields = expected.asMapType().fields()

                    get("map key [%s]") { aMapFields.first() }.isSameFieldAs(eMapFields.first())
                    get("map value [%s]") { aMapFields.last() }.isSameFieldAs(eMapFields.last())
                }
                STRUCT -> {
                    val aStruct = actual.asStructType()
                    val eStruct = expected.asStructType()

                    aStruct.fields().forEach { aField ->
                        val eField = eStruct.field(aField.name())
                        if (eField == null)
                            assert("must NOT contain field \"${aField.name()}\"") {
                                fail(aField, "but found a field [%s]")
                            }
                        else
                            get("field [%s]") { aField }.isSameFieldAs(eField)
                    }
                    eStruct.fields().filter { aStruct.field(it.name()) == null }
                        .forEach { eField ->
                            assert("must contain field [%s]", eField) {
                                fail(eField.name(), "but field %s not found")
                            }
                        }
                }
                else -> {}
            }
        } then {
            if (allPassed) pass() else fail()
        }
    }

/**
 * Assert 2 fields are the same, ignore `fieldId`
 */
fun Assertion.Builder<NestedField>.isSameFieldAs(expected: NestedField): Assertion.Builder<NestedField> =
    compose("is the same as [%s]", expected) {
        get("nullability") { it.isOptional }.isEqualTo(expected.isOptional)
        get("name") { it.name() }.isEqualTo(expected.name())
        get("type %s") { it.type() }.isSameTypeAs(expected.type())
        get("doc") { it.doc() }.isEqualTo(expected.doc())
    } then {
        if (allPassed) pass() else fail()
    }

/**
 * Assert iceberg schema is migrated from `origin` to `actual` match `expected`.
 * It's the same as [isSameTypeAs] but also verify non-change fields id are the same.
 */
private class SchemaMigrateValidation(
    private val ab: Assertion.Builder<Schema>
) {
    private val dot = Joiner.on(".")
    private val fieldNames: Deque<String> = LinkedList() // Stack
    private var fab: Assertion.Builder<*> = ab

    private fun withNode(field: NestedField, block: Assertion.Builder<NestedField>.() -> Unit) {
        val pab = fab
        try {
            fieldNames.push(field.name())
            ab.with("field \"${currentFieldName()}\" (%s)", { field }) {
                fab = this
                this.block()
            }
        } finally {
            fieldNames.pop()
            fab = pab
        }
    }

    fun visit(expected: Schema, actual: Schema, original: Schema) {
        visit(expected.asStruct(), actual.asStruct(), original.asStruct())
    }

    private fun visit(expected: Type, actual: Type, original: Type) {
        when (actual.typeId()) {
            LIST -> list(expected.asListType(), actual.asListType(), original.asListType())
            MAP -> map(expected.asMapType(), actual.asMapType(), original.asMapType())
            STRUCT -> struct(expected.asStructType(), actual.asStructType(), original.asStructType())
            else -> primitive(expected.asPrimitiveType(), actual.asPrimitiveType(), original.asPrimitiveType())
        }
    }

    private fun list(expected: ListType, actual: ListType, origin: ListType) {
        field(expected.fields().first(), actual.fields().first(), origin.fields().first())
    }

    private fun map(expected: MapType, actual: MapType, origin: MapType) {
        field(expected.fields().first(), actual.fields().first(), origin.fields().first())
        field(expected.fields().last(), actual.fields().last(), origin.fields().last())
    }

    private fun struct(expected: StructType, actual: StructType, origin: StructType) {
        expected.fields()
            .filter { actual.field(it.name()) == null }
            .forEach {
                fab.assert("column %s should be added", it.name()) {
                    fail()
                }
            }
        actual.fields()
            .filter { expected.field(it.name()) == null }
            .forEach {
                fab.assert("column %s should be dropped", it.name()) {
                    fail()
                }
            }

        actual.fields().forEach { aField ->
            val eField = expected.field(aField.name())
            val oField = origin.field(aField.name())
            if (eField != null && aField != null)
                field(eField, aField, oField)
        }
    }

    private fun field(expected: NestedField, actual: NestedField, origin: NestedField?) = withNode(actual) {
        // new or replaced field
        if (origin == null || !typeCompatible(origin.type(), expected.type())) {
            isSameFieldAs(expected)
        } else {
            get("id") { actual.fieldId() }.isEqualTo(origin.fieldId())
            get("name") { actual.name() }.isEqualTo(expected.name())
            get("doc") { actual.doc() }.isEqualTo(expected.doc())
            get("nullability") { actual.isOptional }.isEqualTo(expected.isOptional)
            visit(expected.type(), actual.type(), origin.type())
        }
    }

    private fun primitive(expected: PrimitiveType, actual: PrimitiveType, origin: PrimitiveType) {
        fab.get("type %s") { actual }.isSameTypeAs(expected)
    }

    private fun currentFieldName(): String? {
        return if (fieldNames.isEmpty())
            null else
            dot.join(fieldNames.descendingIterator())
    }

    private fun typeCompatible(origin: Type, target: Type): Boolean {
        return when (origin.typeId()) {
            STRUCT -> target.typeId() == STRUCT
            MAP -> target.typeId() == MAP
                && KafkaSchemaUtil.equalsIgnoreId(origin.asMapType().keyType(), target.asMapType().keyType())
                && typeCompatible(origin.asMapType().valueType(), target.asMapType().valueType())
            LIST -> target.typeId() == LIST
                && typeCompatible(origin.asListType().elementType(), target.asListType().elementType())
            else -> target.isPrimitiveType
                && TypeUtil.isPromotionAllowed(origin, target.asPrimitiveType())
        }
    }
}
