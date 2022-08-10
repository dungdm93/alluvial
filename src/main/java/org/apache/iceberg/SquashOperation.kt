package org.apache.iceberg

interface SquashOperation : SnapshotUpdate<SquashOperation> {
    fun squash(lowSnapshotId: Long?, highSnapshotId: Long): SquashOperation
    fun validateDeleteFilesInRange(shouldValidate: Boolean): SquashOperation
    fun add(file: DataFile)
    fun add(file: DeleteFile)
}
