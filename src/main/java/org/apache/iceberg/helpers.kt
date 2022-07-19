package org.apache.iceberg

import org.apache.iceberg.types.Types

fun PartitionSpec.Builder.addSpec(field: Types.NestedField, transform: String): PartitionSpec.Builder {
    return this.addSpec(field, transform, null)
}

fun PartitionSpec.Builder.addSpec(field: Types.NestedField, transform: String, name: String?): PartitionSpec.Builder {
    val targetName = if (name == null || name.isEmpty()) {
        if (transform.contains("[")) {
            field.name() + "_" + transform.substring(0, transform.indexOf('['))
        } else {
            field.name() + "_" + transform
        }
    } else name
    return this.add(field.fieldId(), targetName.lowercase(), transform)
}
