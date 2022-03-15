@file:Suppress("unused")

package dev.alluvial.sink.iceberg.data

import org.apache.iceberg.types.Types.*
import org.apache.iceberg.types.Types.NestedField.optional
import org.apache.iceberg.types.Types.NestedField.required
import strikt.api.expectThat

fun assert2Decimal() {
    val decimal1 = DecimalType.of(9, 0)
    val decimal2 = DecimalType.of(38, 10)
    expectThat(decimal1).isSameTypeAs(decimal2)
}

fun assert2Timestamp() {
    val ts1 = TimestampType.withZone()
    val ts2 = TimestampType.withoutZone()
    expectThat(ts1).isSameTypeAs(ts2)
}

fun assert2Fix() {
    val fix1 = FixedType.ofLength(10)
    val fix2 = FixedType.ofLength(20)
    expectThat(fix1).isSameTypeAs(fix2)
}

fun assert2SimpleList() {
    val list1 = ListType.ofOptional(
        1, StringType.get()
    )
    val list2 = ListType.ofOptional(
        2, LongType.get()
    )
    expectThat(list1).isSameTypeAs(list2)
}

fun assert2ListDiffNullability() {
    val list1 = ListType.ofOptional(
        1, StringType.get()
    )
    val list2 = ListType.ofRequired(
        1, StringType.get()
    )
    expectThat(list1).isSameTypeAs(list2)
}

fun assert2ComplexList() {
    val list1 = ListType.ofOptional(
        1, StructType.of(
            required(1, "first", StringType.get()),
            optional(2, "second", IntegerType.get()),
        )
    )
    val list2 = ListType.ofOptional(
        2, StructType.of(
            required(1, "first", IntegerType.get()),
            optional(2, "second", StringType.get()),
        )
    )
    val list3 = ListType.ofOptional(
        1, StructType.of(
            optional(1, "first", StringType.get()),
            required(2, "second", IntegerType.get()),
        )
    )
    expectThat(list1).isSameTypeAs(list3)
}

fun assert2SimpleMap() {
    val map1 = MapType.ofOptional(
        1, 2, StringType.get(), LongType.get()
    )
    val map2 = MapType.ofOptional(
        2, 1, LongType.get(), StringType.get()
    )
    expectThat(map1).isSameTypeAs(map2)
}

fun assert2MapDiffNullability() {
    val map1 = MapType.ofOptional(
        1, 2, StringType.get(), LongType.get()
    )
    val map2 = MapType.ofRequired(
        1, 2, StringType.get(), LongType.get()
    )
    expectThat(map1).isSameTypeAs(map2)
}

fun assert2ComplexMap() {
    val map1 = MapType.ofOptional(
        1, 2,
        StructType.of(
            required(1, "first", StringType.get()),
            optional(2, "second", DecimalType.of(9, 0)),
        ),
        StructType.of(
            required(3, "third", BooleanType.get()),
            optional(4, "fourth", FloatType.get()),
        )
    )
    val map2 = MapType.ofOptional(  // wrong type in value
        2, 1,
        StructType.of(
            required(1, "first", StringType.get()),
            optional(2, "second", DecimalType.of(9, 0)),
        ),
        StructType.of(
            required(3, "third", FloatType.get()),
            optional(4, "fourth", BooleanType.get()),
        )
    )
    val map3 = MapType.ofOptional( // wrong nullability in value
        1, 2,
        StructType.of(
            required(1, "first", StringType.get()),
            optional(2, "second", DecimalType.of(9, 0)),
        ),
        StructType.of(
            optional(3, "third", BooleanType.get()),
            required(4, "fourth", FloatType.get()),
        )
    )
    val map4 = MapType.ofOptional( // wrong type in key
        1, 2,
        StructType.of(
            required(1, "first", DecimalType.of(9, 0)),
            optional(2, "second", StringType.get()),
        ),
        StructType.of(
            required(3, "third", BooleanType.get()),
            optional(4, "fourth", FloatType.get()),
        )
    )
    val map5 = MapType.ofOptional( // wrong nullability in key
        1, 2,
        StructType.of(
            optional(1, "first", StringType.get()),
            required(2, "second", DecimalType.of(9, 0)),
        ),
        StructType.of(
            required(3, "third", BooleanType.get()),
            optional(4, "fourth", FloatType.get()),
        )
    )
    val map6 = MapType.ofOptional( // wrong nullability in key
        1, 2,
        StructType.of(
            required(1, "first", StringType.get()),
            optional(2, "second", DecimalType.of(38, 10)),
        ),
        StructType.of(
            required(3, "third", BooleanType.get()),
            optional(4, "fourth", FloatType.get()),
        )
    )
    expectThat(map1).isSameTypeAs(map6)
}

fun assert2SimpleStruct() {
    val struct1 = StructType.of(
        required(1, "first", StringType.get()),
        optional(2, "second", IntegerType.get()),
        optional(3, "third", FloatType.get()),
        optional(4, "fourth", BooleanType.get()),
    )
    val struct2 = StructType.of(
        required(1, "first", IntegerType.get()),
        optional(2, "second", StringType.get()),
        optional(3, "third", FloatType.get()),
        optional(5, "fifth", BinaryType.get()),
    )
    expectThat(struct1).isSameTypeAs(struct2)
}

fun assert2StructDiffNullability() {
    val struct1 = StructType.of(
        required(1, "first", StringType.get()),
        optional(2, "second", IntegerType.get()),
    )
    val struct2 = StructType.of(
        optional(1, "first", StringType.get()),
        required(2, "second", IntegerType.get()),
    )
    expectThat(struct1).isSameTypeAs(struct2)
}

fun main() {
    assert2SimpleStruct()
}
