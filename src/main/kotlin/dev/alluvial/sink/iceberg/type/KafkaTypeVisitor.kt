package dev.alluvial.sink.iceberg.type

import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Schema.Type

abstract class KafkaTypeVisitor<T> {
    open fun visit(schema: Schema): T {
        return when (schema.type()) {
            Type.STRUCT -> {
                val fields = schema.fields()
                val fieldResults = buildList(fields.size) {
                    fields.forEach { field ->
                        val fieldSchema = this@KafkaTypeVisitor.visit(field.schema())
                        val fieldResult = this@KafkaTypeVisitor.field(field, fieldSchema)
                        add(fieldResult)
                    }
                }
                this.struct(schema, fieldResults)
            }
            Type.MAP -> {
                val keyResult = this.visit(schema.keySchema())
                val valueResult = this.visit(schema.valueSchema())
                this.map(schema, keyResult, valueResult)
            }
            Type.ARRAY -> {
                val elementResult = this.visit(schema.valueSchema())
                this.array(schema, elementResult)
            }
            else -> this.primitive(schema)
        }
    }

    abstract fun struct(schema: Schema, fieldResults: List<T>): T
    abstract fun field(field: Field, fieldSchema: T): T
    abstract fun map(schema: Schema, keyResult: T, valueResult: T): T
    abstract fun array(schema: Schema, elementResult: T): T
    abstract fun primitive(schema: Schema): T
}
