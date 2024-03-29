package org.apache.iceberg

import org.apache.iceberg.common.DynFields
import org.apache.iceberg.common.DynMethods
import org.apache.iceberg.metrics.MetricsReporter
import org.slf4j.LoggerFactory

class AlluvialTransaction(
    tableName: String,
    ops: TableOperations,
    start: TableMetadata
) : BaseTransaction(tableName, ops, TransactionType.SIMPLE, start) {
    companion object {
        private val logger = LoggerFactory.getLogger(AlluvialTransaction::class.java)

        private val updatesField = DynFields.builder()
            .hiddenImpl(BaseTransaction::class.java, "updates")
            .build<MutableList<PendingUpdate<*>>>()
        private val reporterField = DynFields.builder()
            .hiddenImpl(BaseTransaction::class.java, "reporter")
            .build<MetricsReporter>()
        private val checkLastOperationCommittedMethod = DynMethods.builder("checkLastOperationCommitted")
            .hiddenImpl(BaseTransaction::class.java, String::class.java)
            .build()

        fun of(table: Table): AlluvialTransaction {
            val ops = (table as HasTableOperations).operations()
            return AlluvialTransaction(table.name(), ops, ops.refresh())
        }
    }

    private val updates = updatesField.get(this)
    private val reporter = reporterField.get(this)

    @Suppress("SameParameterValue")
    private fun checkLastOperationCommitted(operation: String): Unit =
        // invoke org.apache.iceberg.BaseTransaction.checkLastOperationCommitted which is private method
        checkLastOperationCommittedMethod.invoke(this, operation)

    fun squash(): SquashOperation {
        checkLastOperationCommitted("SquashOperation")
        val table = table() as TransactionTable

        val squash = AlluvialSquashOperation(table.name(), table.operations()).reportWith(reporter)
        updates.add(squash)

        return squash
    }
}
