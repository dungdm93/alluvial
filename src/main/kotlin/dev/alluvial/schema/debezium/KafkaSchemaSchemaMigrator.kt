package dev.alluvial.schema.debezium

import dev.alluvial.sink.iceberg.type.IcebergField
import dev.alluvial.sink.iceberg.type.IcebergSchema
import dev.alluvial.sink.iceberg.type.IcebergType
import dev.alluvial.sink.iceberg.type.IcebergTypeID
import dev.alluvial.sink.iceberg.type.KafkaField
import dev.alluvial.sink.iceberg.type.KafkaSchema
import dev.alluvial.sink.iceberg.type.KafkaType
import dev.alluvial.sink.iceberg.type.isPromotionAllowed
import dev.alluvial.sink.iceberg.type.toIcebergType
import org.apache.iceberg.UpdateSchema
import org.apache.iceberg.relocated.com.google.common.base.Joiner
import org.apache.iceberg.relocated.com.google.common.base.Preconditions
import org.apache.iceberg.types.Type.PrimitiveType
import org.apache.iceberg.types.Types.*
import org.slf4j.LoggerFactory
import java.util.Deque
import java.util.LinkedList

class KafkaSchemaSchemaMigrator(
    private val schemaUpdater: UpdateSchema,
) {
    companion object {
        private val logger = LoggerFactory.getLogger(KafkaSchemaSchemaMigrator::class.java)
        private val DOT = Joiner.on(".")
    }

    private val fieldNames: Deque<String> = LinkedList() // Stack

    fun visit(sSchema: KafkaSchema, iSchema: IcebergSchema) {
        visit(sSchema, iSchema.asStruct())
    }

    fun visit(sType: KafkaSchema, iType: IcebergType) {
        when (iType.typeId()) {
            IcebergTypeID.LIST -> list(sType, iType.asListType())
            IcebergTypeID.MAP -> map(sType, iType.asMapType())
            IcebergTypeID.STRUCT -> struct(sType, iType.asStructType())
            else -> primitive(sType, iType.asPrimitiveType())
        }
    }

    private inline fun withNode(name: String, block: () -> Unit) {
        try {
            fieldNames.push(name)
            return block()
        } finally {
            fieldNames.pop()
        }
    }

    private fun list(sList: KafkaSchema, iList: ListType) {
        Preconditions.checkArgument(sList.type() == KafkaType.ARRAY, "sList must be ARRAY")

        withNode("element") {
            val sElementSchema = sList.valueSchema()
            if (sElementSchema.isOptional != iList.isElementOptional)
                updateNullability(sElementSchema.isOptional)
            visit(sElementSchema, iList.elementType())
        }
    }

    private fun map(sMap: KafkaSchema, iMap: MapType) {
        Preconditions.checkArgument(sMap.type() == KafkaType.MAP, "sMap must be MAP")

        withNode("key") {
            visit(sMap.keySchema(), iMap.keyType())
        }
        withNode("value") {
            val sValueSchema = sMap.valueSchema()
            if (sValueSchema.isOptional != iMap.isValueOptional)
                updateNullability(sValueSchema.isOptional)
            visit(sValueSchema, iMap.valueType())
        }
    }

    private fun struct(sStruct: KafkaSchema, iStruct: StructType) {
        Preconditions.checkArgument(sStruct.type() == KafkaType.STRUCT, "sStruct must be STRUCT")

        iStruct.fields().forEach { iField ->
            val sField = sStruct.field(iField.name())
            if (sField == null) {
                dropColumn(iField)
            } else {
                field(sField, iField)
            }
        }
        sStruct.fields()
            .filter { iStruct.field(it.name()) == null }
            .forEach(::addColumn)
    }

    private fun field(sField: KafkaField?, iField: IcebergField?) {
        if (sField == null && iField == null) throw IllegalArgumentException("Either sField or iField must be non-null")
        if (sField == null) return dropColumn(iField!!)
        if (iField == null) return addColumn(sField)

        if (!sField.isPromotionAllowed(iField)) {
            return replaceColumn(sField, iField)
        }
        withNode(iField.name()) {
            val sSchema = sField.schema()
            if (sSchema.doc() != iField.doc())
                updateDoc(sSchema.doc())
            if (sSchema.isOptional != iField.isOptional)
                updateNullability(sSchema.isOptional)
            visit(sField.schema(), iField.type())
        }
    }

    private fun primitive(sPrimitive: KafkaSchema, iPrimitive: PrimitiveType) {
        // expect sPrimitive will be converted to iceberg primitive type, otherwise raise an IllegalArgumentException
        val ePrimitive = sPrimitive.toIcebergType().asPrimitiveType()
        if (iPrimitive == ePrimitive) return

        val currentFieldName = currentFieldName()
        logger.warn("Change column \"{}\" datatype to: {}", currentFieldName, ePrimitive)
        // expect ePrimitive is compatible with iPrimitive, otherwise raise an IllegalArgumentException
        schemaUpdater.updateColumn(currentFieldName, ePrimitive, sPrimitive.doc())
    }

    private fun addColumn(sField: KafkaField) {
        val parentFieldName = currentFieldName()
        val currentFieldShortName = sField.name()
        val currentFieldName = if (parentFieldName == null)
            currentFieldShortName else
            "$parentFieldName.${currentFieldShortName}"
        val sType = sField.schema()
        val iType = sType.toIcebergType()

        if (sType.isOptional) {
            logger.warn("Add optional column \"{}\" with type: {}", currentFieldName, iType)
            schemaUpdater.addColumn(parentFieldName, currentFieldShortName, iType, sType.doc())
        } else {
            logger.warn("Add required column \"{}\" with type: {}", currentFieldName, iType)
            schemaUpdater.allowIncompatibleChanges()
            schemaUpdater.addRequiredColumn(parentFieldName, currentFieldShortName, iType, sType.doc())
        }
    }

    private fun dropColumn(iField: IcebergField) = withNode(iField.name()) {
        val currentFieldName = currentFieldName()

        logger.warn("Drop column \"{}\"", currentFieldName)
        schemaUpdater.deleteColumn(currentFieldName)
    }

    private fun replaceColumn(sField: KafkaField, iField: IcebergField) {
        dropColumn(iField)
        addColumn(sField)
    }

    private fun updateDoc(doc: String?) {
        val currentFieldName = currentFieldName()

        logger.warn("Update column \"{}\" doc to: {}", currentFieldName, doc)
        schemaUpdater.updateColumnDoc(currentFieldName, doc)
    }

    private fun updateNullability(optional: Boolean) {
        val currentFieldName = currentFieldName()

        if (optional) {
            logger.warn("Change column \"{}\" nullability from required -> optional", currentFieldName)
            schemaUpdater.makeColumnOptional(currentFieldName)
        } else {
            logger.warn("Change column \"{}\" nullability from optional -> required", currentFieldName)
            schemaUpdater.allowIncompatibleChanges()
            schemaUpdater.requireColumn(currentFieldName)
        }
    }

    private fun currentFieldName(): String? {
        return if (fieldNames.isEmpty())
            null else
            DOT.join(fieldNames.descendingIterator())
    }
}
