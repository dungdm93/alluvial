package org.apache.iceberg;

import org.apache.iceberg.PartitionSpec.Builder;
import org.apache.iceberg.types.Types;

public class PartitionSpecs {
    public static void add(Builder builder, Types.NestedField field, String transform) {
        String targetName;
        if (transform.contains("[")) {
            targetName = field.name() + "_" + transform.substring(0, transform.indexOf('['));
        } else {
            targetName = field.name() + "_" + transform;
        }
        builder.add(field.fieldId(), targetName, transform);
    }
}
