package dev.alluvial.dedupe.backend.rocksdb

import com.google.common.base.Preconditions
import dev.alluvial.sink.iceberg.type.KafkaStruct
import org.apache.kafka.connect.sink.SinkRecord
import java.lang.StringBuilder

class BasicRecordSerializer(private val identifiers: List<String>, private val delimiter: String = "-") :
    RocksDbRecordSerializer<SinkRecord> {
    override fun serialize(record: SinkRecord): ByteArray {
        Preconditions.checkArgument(record.key() != null, "Record key must be non null")
        val keyStruct = record.key() as KafkaStruct
        val builder = StringBuilder()
        identifiers.forEach { idName ->
            val idValue = keyStruct.get(idName)
            if (builder.isNotEmpty()) {
                builder.append(delimiter)
            }
            builder.append(idValue.toString())
        }
        return builder.toString().encodeToByteArray()
    }
}
