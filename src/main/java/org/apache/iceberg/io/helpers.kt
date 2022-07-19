package org.apache.iceberg.io

import org.apache.iceberg.StructLike

fun StructLike?.copy(): StructLike? {
    return StructCopy.copy(this)
}
