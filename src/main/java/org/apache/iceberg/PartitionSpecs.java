package org.apache.iceberg;

import org.apache.iceberg.PartitionSpec.Builder;
import org.apache.iceberg.types.Types;

import java.util.Locale;

public class PartitionSpecs {
    public static void add(Builder builder, Types.NestedField field, String transform, String name) {
        String targetName = name;
        if (name == null || name.isEmpty()) {
            if (transform.contains("[")) {
                targetName = field.name() + "_" + transform.substring(0, transform.indexOf('['));
            } else {
                targetName = field.name() + "_" + transform;
            }
        }
        builder.add(field.fieldId(), targetName.toLowerCase(), transform);
    }

    public static void add(Builder builder, Types.NestedField field, String transform) {
        add(builder, field, transform, null);
    }
}
