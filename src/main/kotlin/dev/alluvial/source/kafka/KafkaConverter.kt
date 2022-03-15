package dev.alluvial.source.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.header.ConnectHeaders
import org.apache.kafka.connect.header.Headers
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.storage.Converter
import org.apache.kafka.connect.storage.HeaderConverter
import org.slf4j.LoggerFactory
import java.io.Closeable

@Suppress("MemberVisibilityCanBePrivate")
class KafkaConverter(config: Map<String, Any>) : Closeable {
    companion object {
        private val logger = LoggerFactory.getLogger(KafkaConverter::class.java)

        // See: WorkerConfig
        const val KEY_CONVERTER_CLASS_CONFIG = "key.converter"
        const val VALUE_CONVERTER_CLASS_CONFIG = "value.converter"
        const val HEADER_CONVERTER_CLASS_CONFIG = "header.converter"
        const val HEADER_CONVERTER_CLASS_DEFAULT = "org.apache.kafka.connect.storage.SimpleHeaderConverter"

        fun newConverter(config: Map<String, Any>, classPropertyName: String, isKey: Boolean): Converter {
            require(config.containsKey(classPropertyName)) { "Require '$classPropertyName' in config" }
            val className: String = config[classPropertyName] as String
            val jClass = Class.forName(className)
            val converter = jClass.getDeclaredConstructor().newInstance() as Converter

            val prefix = "${classPropertyName}."
            val converterConfig = config.filterKeys { it.startsWith(prefix) }
                .mapKeys { (k, _) -> k.removePrefix(prefix) }
            converter.configure(converterConfig, isKey)

            return converter
        }

        fun newHeaderConverter(config: Map<String, Any>, classPropertyName: String): HeaderConverter {
            val className: String = config.getOrDefault(classPropertyName, HEADER_CONVERTER_CLASS_DEFAULT) as String
            val jClass = Class.forName(className)
            val converter = jClass.getDeclaredConstructor().newInstance() as HeaderConverter

            val prefix = "${classPropertyName}."
            val converterConfig = config.filterKeys { it.startsWith(prefix) }
                .mapKeys { (k, _) -> k.removePrefix(prefix) }
            converter.configure(converterConfig)

            return converter
        }
    }

    private val keyConverter: Converter
    private val valueConverter: Converter
    private val headerConverter: HeaderConverter

    init {
        keyConverter = newConverter(config, KEY_CONVERTER_CLASS_CONFIG, true)
        valueConverter = newConverter(config, VALUE_CONVERTER_CLASS_CONFIG, false)
        headerConverter = newHeaderConverter(config, HEADER_CONVERTER_CLASS_CONFIG)
    }

    fun convert(message: ConsumerRecord<ByteArray, ByteArray>): SinkRecord {
        val keyAndSchema = convertKey(message)
        val valueAndSchema = convertValue(message)
        val headers = convertHeaders(message)

        return SinkRecord(
            message.topic(), message.partition(),
            keyAndSchema.schema(), keyAndSchema.value(),
            valueAndSchema.schema(), valueAndSchema.value(),
            message.offset(), message.timestamp(), message.timestampType(),
            headers
        )
    }

    fun convertKey(message: ConsumerRecord<ByteArray, ByteArray>): SchemaAndValue {
        try {
            return keyConverter.toConnectData(message.topic(), message.headers(), message.key())
        } catch (e: Exception) {
            logger.error(
                "Error converting message key in topic '{}' partition {} at offset {} and timestamp {}: {}",
                message.topic(), message.partition(), message.offset(), message.timestamp(), e.message, e
            )
            throw e
        }
    }

    fun convertValue(message: ConsumerRecord<ByteArray, ByteArray>): SchemaAndValue {
        try {
            return valueConverter.toConnectData(message.topic(), message.headers(), message.value())
        } catch (e: Exception) {
            logger.error(
                "Error converting message value in topic '{}' partition {} at offset {} and timestamp {}: {}",
                message.topic(), message.partition(), message.offset(), message.timestamp(), e.message, e
            )
            throw e
        }
    }

    fun convertHeaders(message: ConsumerRecord<ByteArray, ByteArray>): Headers {
        val result: Headers = ConnectHeaders()
        val recordHeaders = message.headers() ?: return result

        val topic = message.topic()
        for (recordHeader in recordHeaders) {
            val schemaAndValue: SchemaAndValue =
                headerConverter.toConnectHeader(topic, recordHeader.key(), recordHeader.value())
            result.add(recordHeader.key(), schemaAndValue)
        }
        return result
    }

    override fun close() {
        if (keyConverter is Closeable) keyConverter.close()
        if (valueConverter is Closeable) valueConverter.close()
        headerConverter.close()
    }
}
