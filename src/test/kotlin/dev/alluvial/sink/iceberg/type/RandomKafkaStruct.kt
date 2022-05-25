package dev.alluvial.sink.iceberg.type

import org.apache.iceberg.types.TypeUtil

object RandomKafkaStruct {
    fun generate(iSchema: IcebergSchema, numRecords: Int, seed: Long): Iterable<KafkaStruct> {
        val sSchema = iSchema.toKafkaSchema()
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
        val sSchema = iSchema.toKafkaSchema()
        return convert(iSchema, sSchema, records)
    }

    fun convert(iSchema: IcebergSchema, sSchema: KafkaSchema, records: Iterable<IcebergRecord>): Iterable<KafkaStruct> {
        val converter = KafkaStructConverter(sSchema)
        return records.map { converter.convert(iSchema, it) }
    }

    fun convert(iSchema: IcebergSchema, record: IcebergRecord): KafkaStruct {
        val sSchema = iSchema.toKafkaSchema()
        val converter = KafkaStructConverter(sSchema)
        return converter.convert(iSchema, record)
    }
}
