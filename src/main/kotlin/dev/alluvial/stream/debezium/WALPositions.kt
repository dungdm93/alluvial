package dev.alluvial.stream.debezium

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeInfo.As.PROPERTY
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id.NAME
import dev.alluvial.sink.iceberg.type.KafkaStruct
import java.util.regex.Pattern

@JsonTypeInfo(use = NAME, include = PROPERTY, property = "connector")
@JsonSubTypes(
    JsonSubTypes.Type(value = MySqlWALPosition::class, name = "mysql"),
    JsonSubTypes.Type(value = PostgresWALPosition::class, name = "postgresql"),
)
interface WALPosition : Comparable<WALPosition> {
    fun fromSource(source: KafkaStruct): WALPosition
    operator fun compareTo(other: KafkaStruct): Int
}

data class MySqlWALPosition(
    @get:JsonProperty("file-index")
    val fileIndex: Long,
    val pos: Long,
) : WALPosition {
    companion object {
        private val pattern = Pattern.compile("^.*\\.(\\d+)$")
    }

    override fun fromSource(source: KafkaStruct): WALPosition {
        val file = extractFile(source)
        val pos = extractPos(source)
        return MySqlWALPosition(file, pos)
    }

    override fun compareTo(other: WALPosition): Int {
        val that = other as MySqlWALPosition
        val res = this.fileIndex.compareTo(that.fileIndex)
        return if (res != 0)
            res else
            this.pos.compareTo(that.pos)
    }

    override fun compareTo(other: KafkaStruct): Int {
        val res = this.fileIndex.compareTo(extractFile(other))
        return if (res != 0)
            res else
            this.pos.compareTo(extractPos(other))
    }

    private fun extractFile(source: KafkaStruct): Long {
        val file = source.getString("file")
        val m = pattern.matcher(file)
        assert(m.matches()) { "invalid source.file" }
        return m.group(1).toLong()
    }

    private fun extractPos(source: KafkaStruct): Long {
        return source.getInt64("pos")
    }
}

data class PostgresWALPosition(
    val lsn: Long
) : WALPosition {
    override fun fromSource(source: KafkaStruct): WALPosition {
        val lsn = source.getInt64("lsn")
        return PostgresWALPosition(lsn)
    }

    override fun compareTo(other: WALPosition): Int {
        val that = other as PostgresWALPosition
        return this.lsn.compareTo(that.lsn)
    }

    override fun compareTo(other: KafkaStruct): Int {
        val thatLsn = other.getInt64("lsn")
        return lsn.compareTo(thatLsn)
    }
}
