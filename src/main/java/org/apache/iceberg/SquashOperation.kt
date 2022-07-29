package org.apache.iceberg

import dev.alluvial.sink.iceberg.filter
import dev.alluvial.sink.iceberg.io.GenericReader
import org.apache.iceberg.DataOperations.*
import org.apache.iceberg.FileContent.*
import org.apache.iceberg.MetadataColumns.*
import org.apache.iceberg.SnapshotRef.MAIN_BRANCH
import org.apache.iceberg.SnapshotSummary.*
import org.apache.iceberg.exceptions.ValidationException
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.relocated.com.google.common.base.Preconditions
import org.apache.iceberg.util.Tasks
import java.util.Objects

internal class SquashOperation(
    private val tableName: String,
    private val ops: TableOperations,
) : MergingSnapshotProducer<SquashOperation>(tableName, ops) {
    private val io = ops.io()

    private var lowSnapshot: Snapshot? = null
    private lateinit var highSnapshot: Snapshot
    private lateinit var operation: String
    private lateinit var rollback: PendingUpdate<*>
    private var cherrypickUpdates = emptyList<SnapshotProducer<*>>()
    private val cherrypickMap = mutableMapOf<Long, SnapshotProducer<*>>()

    private var validated = false
    private var validatePosDeletesFilesInRange = true

    private lateinit var aggAddedDataFiles: Set<DataFile>
    private lateinit var aggRemovedDataFiles: Set<DataFile>
    private lateinit var aggAddedDeleteFiles: Set<DeleteFile>
    private lateinit var aggRemovedDeleteFiles: Set<DeleteFile>

    // files added in compaction group but no longer exist after squash
    private val deadFiles = mutableSetOf<ContentFile<*>>()

    override fun self() = this

    override fun operation() = operation

    fun squash(lowSnapshotId: Long?, highSnapshotId: Long): SquashOperation {
        val base = current()
        lowSnapshot = lowSnapshotId?.let(base::snapshot)
        highSnapshot = base.snapshot(highSnapshotId)

        rollback = if (lowSnapshotId != null) {
            SetSnapshotOperation(ops)
                .rollbackTo(lowSnapshotId)
        } else {
            RemoveBranch()
                .name(MAIN_BRANCH)
        }

        val ancestors = base.ancestorsOf(highSnapshotId)
            .filterAfter(lowSnapshot)
            .reversed()

        setFiles(ancestors)
        setSummary(ancestors)
        operation = determineOperator(ancestors)
        return this
    }

    fun validateDeleteFilesInRange(shouldValidate: Boolean): SquashOperation {
        validatePosDeletesFilesInRange = shouldValidate
        return this
    }

    public override fun add(file: DataFile) {
        deadFiles.remove(file)
        super.add(file)
    }

    public override fun add(file: DeleteFile) {
        Preconditions.checkArgument(
            file.content() == EQUALITY_DELETES,
            "Expected only add EQUALITY_DELETES for now, got %s", file
        )
        deadFiles.remove(file)
        super.add(file)
    }

    override fun validate(currentMetadata: TableMetadata) {
        if (validated) return // no need to re-validate
        if (validatePosDeletesFilesInRange)
            validatePosDeletesReferenceToDataFileInRange(currentMetadata)
        validated = true
    }

    override fun commit() {
        val base = refresh()

        cherrypickUpdates = base.currentAncestors()
            .filterAfter(highSnapshot)
            .reversed()
            .map { cherrypickMap.computeIfAbsent(it.snapshotId(), this::cherrypick) }

        rollback.commit()
        if (operation != NOOP) super.commit()
        cherrypickUpdates.forEach { it.commit() }

        cleanupOnCommitSuccess()
    }

    override fun cleanAll() {
        super.cleanAll()

        Tasks.foreach(cherrypickMap.values)
            .suppressFailureWhenFinished()
            .run(SnapshotProducer<*>::cleanAll)
    }

    private fun validatePosDeletesReferenceToDataFileInRange(current: TableMetadata) {
        val ancestors = current.ancestorsOf(highSnapshot)
            .filterAfter(lowSnapshot)
            .reversed()

        val dataFiles = mutableSetOf<DataFile>()
        ancestors.forEach { snapshot ->
            dataFiles.addAll(snapshot.addedDataFiles(io))
            val posDelFiles = snapshot.addedDeleteFiles(io)
                .filter { it.content() == POSITION_DELETES }

            val filter = Expressions.notIn(
                DELETE_FILE_PATH.name(),
                *dataFiles.map(DataFile::path).toTypedArray()
            )
            val records = GenericReader(io, POS_DELETE_SCHEMA)
                .openFile(posDelFiles, filter)

            if (records.any()) {
                throw ValidationException(
                    "Snapshot %s contains POSITION_DELETES file reference to out of CompactionGroup",
                    snapshot.snapshotId()
                )
            }
        }
    }

    private fun setFiles(ancestors: List<Snapshot>) {
        val aggAddedDataFiles = mutableSetOf<DataFile>()
        val aggRemovedDataFiles = mutableSetOf<DataFile>()
        val aggAddedDeleteFiles = mutableSetOf<DeleteFile>()
        val aggRemovedDeleteFiles = mutableSetOf<DeleteFile>()

        ancestors.forEach { snapshot ->
            aggAddedDataFiles.addAll(snapshot.addedDataFiles(io))
            aggAddedDeleteFiles.addAll(snapshot.addedDeleteFiles(io))
            snapshot.removedDataFiles(io).forEach {
                val present = aggAddedDataFiles.remove(it)
                if (!present) aggRemovedDataFiles.add(it)
            }
            snapshot.removedDeleteFiles(io).forEach {
                val present = aggAddedDeleteFiles.remove(it)
                if (!present) aggRemovedDeleteFiles.add(it)
            }
        }
        this.aggAddedDataFiles = aggAddedDataFiles
        this.aggRemovedDataFiles = aggRemovedDataFiles
        this.aggAddedDeleteFiles = aggAddedDeleteFiles
        this.aggRemovedDeleteFiles = aggRemovedDeleteFiles

        deadFiles.addAll(aggAddedDataFiles)
        deadFiles.addAll(aggAddedDeleteFiles)

        aggRemovedDataFiles.forEach(::delete)
        aggRemovedDeleteFiles.forEach(::delete)
        failMissingDeletePaths()
    }

    private fun setSummary(ancestors: List<Snapshot>) {
        set(SQUASH_SNAPSHOT_ID_PROP, "(${lowSnapshot?.snapshotId() ?: ""}..${highSnapshot.snapshotId()}]")

        val originalSnapshotTs = highSnapshot.originalTimestampMillis()
        set(ORIGINAL_SNAPSHOT_TS_PROP, originalSnapshotTs.toString())

        ancestors.forEach { snapshot ->
            snapshot.extraMetadata().forEach(::set)
        }
    }

    private fun determineOperator(ancestors: List<Snapshot>): String {
        if (ancestors.all { it.operation() == REPLACE }) {
            return REPLACE
        }

        if (aggAddedDataFiles.isEmpty() &&
            aggRemovedDataFiles.isEmpty() &&
            aggAddedDeleteFiles.isEmpty() &&
            aggRemovedDeleteFiles.isEmpty()
        ) return NOOP // zero aggregate changes

        // Only append new DATA files
        if (aggRemovedDataFiles.isEmpty() &&
            aggAddedDeleteFiles.isEmpty() &&
            aggRemovedDeleteFiles.isEmpty()
        ) return APPEND

        // Not append any new files
        if (aggAddedDataFiles.isEmpty() &&
            aggAddedDeleteFiles.isEmpty()
        ) return DELETE

        return OVERWRITE
    }

    private fun cleanupOnCommitSuccess() {
        val abortUpdates = cherrypickMap.values.toSet() - cherrypickUpdates.toSet()
        Tasks.foreach(abortUpdates)
            .suppressFailureWhenFinished()
            .run(SnapshotProducer<*>::cleanAll)
    }

    private fun cherrypick(snapshotId: Long): SnapshotProducer<*> {
        return SquashCherrypickOperation()
            .deleteWith(::deleteFile)
            .cherrypick(snapshotId)
    }

    inner class SquashCherrypickOperation : MergingSnapshotProducer<SquashCherrypickOperation>(tableName, ops) {
        private var cherrypickSnapshot: Snapshot? = null
        private var requireFastForward = false // TODO
        private var validated: Int? = null

        override fun self() = this

        override fun operation(): String {
            if (cherrypickSnapshot == null) {
                throw IllegalStateException("No cherry-pick snapshot")
            }
            return cherrypickSnapshot!!.operation()
        }

        /**
         * @see org.apache.iceberg.CherryPickOperation.cherrypick
         */
        fun cherrypick(snapshotId: Long): SquashCherrypickOperation {
            if (cherrypickSnapshot != null) {
                throw IllegalStateException("Already cherry-picked snapshot ${cherrypickSnapshot?.snapshotId()}")
            }
            val cs = current().snapshot(snapshotId)
                ?: throw ValidationException("Cannot cherry-pick unknown snapshot ID: %s", snapshotId)

            setFiles(cs)
            setSummary(cs)
            cherrypickSnapshot = cs

            return this
        }

        override fun apply(): Snapshot {
            val base = refresh()
            if (cherrypickSnapshot == null) {
                // if no target snapshot was configured then NOOP by returning current state
                return base.currentSnapshot()
            }

            val cs = cherrypickSnapshot!!
            val isFastForward = base.isFastForward(cs)
            if (requireFastForward || isFastForward) {
                if (!isFastForward) {
                    throw ValidationException(
                        "Cannot cherry-pick snapshot %s: not append, dynamic overwrite, or fast-forward",
                        cs.snapshotId()
                    )
                }
                return cs
            }

            return super.apply()
        }

        override fun validate(currentMetadata: TableMetadata) {
            val hash = Objects.hashCode(deadFiles)
            if (hash == validated) return // no need to re-validate
            validated = hash

            val cs = cherrypickSnapshot ?: return
            val posDel = cs.addedDeleteFiles(io)
                .filter { it.content() == POSITION_DELETES }

            val filter = Expressions.`in`(
                DELETE_FILE_PATH.name(),
                *deadFiles.map(ContentFile<*>::path).toTypedArray()
            )
            val records = GenericReader(io, POS_DELETE_SCHEMA)
                .openFile(posDel, filter)

            if (records.any()) {
                throw ValidationException("Found POSITION_DELETES reference to the dead files in CompactionGroup")
            }
        }

        private fun setFiles(snapshot: Snapshot) {
            snapshot.addedDataFiles(io).forEach(::add)
            snapshot.addedDeleteFiles(io).forEach(::add)
            snapshot.removedDataFiles(io).forEach(::delete)
            snapshot.removedDeleteFiles(io).forEach(::delete)
            // check that all deleted files are still in the table
            failMissingDeletePaths()
        }

        private fun setSummary(snapshot: Snapshot) {
            set(SOURCE_SNAPSHOT_ID_PROP, snapshot.snapshotId().toString())

            val originalSnapshotTs = snapshot.originalTimestampMillis()
            set(ORIGINAL_SNAPSHOT_TS_PROP, originalSnapshotTs.toString())

            snapshot.extraMetadata().forEach(::set)
        }
    }

    /**
     * `UpdateSnapshotReferencesOperation` is not allowed to remove `main` branch
     * orphan branch (snapshotId = -1) can't be created either
     * @see org.apache.iceberg.UpdateSnapshotReferencesOperation.removeBranch
     * @see org.apache.iceberg.TableMetadata.Builder.setRef
     */
    inner class RemoveBranch : PendingUpdate<SnapshotRef> {
        private lateinit var name: String

        fun name(branchName: String): RemoveBranch {
            this.name = branchName
            return this
        }

        override fun apply(): SnapshotRef {
            return SnapshotRef.branchBuilder(-1).build()
        }

        override fun commit() {
            val base = refresh()
            val updated = TableMetadata.buildFrom(base)
                .removeRef(name)
                .build()

            ops.commit(base, updated)
        }
    }
}
