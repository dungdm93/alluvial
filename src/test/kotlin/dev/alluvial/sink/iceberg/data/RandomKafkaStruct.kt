package dev.alluvial.sink.iceberg.data

import org.apache.iceberg.types.TypeUtil
import org.apache.iceberg.Schema as IcebergSchema
import org.apache.iceberg.data.Record as IcebergRecord
import org.apache.kafka.connect.data.Schema as KafkaSchema
import org.apache.kafka.connect.data.Struct as KafkaStruct

object RandomKafkaStruct {
    fun generate(iSchema: IcebergSchema, numRecords: Int, seed: Long): Iterable<KafkaStruct> {
        val sSchema = KafkaSchemaUtil.toKafkaSchema(iSchema)
        val generator = KafkaRandomDataGenerator.BasedOnIcebergSchema(seed, sSchema)
        return Iterable {
            iterator {
                repeat(numRecords) {
                    val struct = TypeUtil.visit(iSchema, generator) as KafkaStruct
                    yield(struct)
                }
            }
        }
    }

    fun generate(sSchema: KafkaSchema, numRecords: Int, seed: Long): Iterable<KafkaStruct> {
        val generator = KafkaRandomDataGenerator.BasedOnKafkaSchema(seed)
        return Iterable {
            iterator {
                repeat(numRecords) {
                    val struct = generator.generate(sSchema) as KafkaStruct
                    yield(struct)
                }
            }
        }
    }

    fun convert(iSchema: IcebergSchema, records: Iterable<IcebergRecord>): Iterable<KafkaStruct> {
        val sSchema = KafkaSchemaUtil.toKafkaSchema(iSchema)
        val converter = KafkaStructConverter(sSchema)
        return records.map { converter.convert(iSchema, it) }
    }

    fun convert(iSchema: IcebergSchema, record: IcebergRecord): KafkaStruct {
        val sSchema = KafkaSchemaUtil.toKafkaSchema(iSchema)
        val converter = KafkaStructConverter(sSchema)
        return converter.convert(iSchema, record)
    }
}
