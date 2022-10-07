package dev.alluvial.dedupe.backend.rocksdb

import com.google.common.base.Preconditions
import dev.alluvial.dedupe.DedupeBackend
import dev.alluvial.runtime.DeduplicationConfig
import org.rocksdb.*
import org.slf4j.LoggerFactory
import org.rocksdb.ColumnFamilyOptions as CFOptions

class RocksDbClient private constructor(val path: String, private val options: Options) :
    DedupeBackend<ByteArray, ByteArray> {
    companion object {
        init {
            RocksDB.loadLibrary()
        }

        private val logger = LoggerFactory.getLogger(RocksDbClient::class.java)
        private const val DATABASE_OPT_PREFIX = "db."
        private const val COLUMN_FAMILY_OPT_PREFIX = "cf."
        private var instance: RocksDbClient? = null

        fun getOrCreate(config: DeduplicationConfig): RocksDbClient {
            Preconditions.checkArgument(
                config.path.isNotEmpty(),
                "RocksDB path must not be empty"
            )
            val options = buildOptions(config.properties)

            if (instance == null) {
                instance = RocksDbClient(config.path, options)
            } else if (instance!!.path != config.path) {
                throw IllegalArgumentException("RocksDB instance has been initialized with different path")
            }
            return instance!!
        }

        private fun buildOptions(config: Map<String, String>): Options {
            val dbProps = config.filterConfig(DATABASE_OPT_PREFIX).toProperties()
            val dbOptions = if (dbProps.isEmpty) DBOptions() else DBOptions.getDBOptionsFromProps(dbProps)
            // Hard-coded options
            dbOptions.setCreateIfMissing(true)
            dbOptions.setCreateMissingColumnFamilies(true)

            val cfProps = config.filterConfig(COLUMN_FAMILY_OPT_PREFIX).toProperties()
            val cfOptions =
                if (cfProps.isEmpty) CFOptions() else CFOptions.getColumnFamilyOptionsFromProps(cfProps)
            return Options(dbOptions, cfOptions)
        }

        private fun String.toCfName() = this.toByteArray(Charsets.UTF_8)

        private fun ByteArray.toTableName() = String(this, Charsets.UTF_8)

        private fun <V> Map<String, V>.filterConfig(prefix: String): Map<String, V> {
            return this.filterKeys { it.startsWith(prefix) }.mapKeys { it.key.substring(prefix.length) }
        }
    }

    private lateinit var db: RocksDB
    private val cfHandles = mutableMapOf<String, ColumnFamilyHandle>()

    init {
        initDB()
    }

    private fun initDB() {
        val existingCfNames = RocksDB.listColumnFamilies(options, path)

        if (existingCfNames.isEmpty()) {
            db = RocksDB.open(options, path)
        } else {
            val descriptors = existingCfNames.map { ColumnFamilyDescriptor(it) }
            val handles = mutableListOf<ColumnFamilyHandle>()

            db = RocksDB.open(DBOptions(options), path, descriptors, handles)
            cfHandles.putAll(existingCfNames.zip(handles).map { (k, v) -> Pair(k.toTableName(), v) })
        }
    }

    override fun createTableIfNeeded(table: String) {
        logger.info("Create table '{}' if not exist", table)
        cfHandles.computeIfAbsent(table) { name ->
            val descriptor = ColumnFamilyDescriptor(name.toCfName())
            db.createColumnFamily(descriptor)
        }
    }

    private fun tableHandle(table: String): ColumnFamilyHandle {
        return cfHandles[table] ?: throw IllegalArgumentException("RocksDB table $table does not exist")
    }

    override fun dropTableIfExists(table: String) {
        logger.info("Drop table '{}' if exists", table)
        cfHandles.remove(table)?.let(db::dropColumnFamily)
    }

    override fun listTables(): List<String> = cfHandles.keys.toList()

    override fun refresh() {
        close()
        initDB()
    }

    override fun close() {
        db.closeE()
        options.close()
        instance = null
    }

    override fun delete(table: String, key: ByteArray) {
        val handle = tableHandle(table)
        db.delete(handle, key)
    }

    override fun hasKey(table: String, key: ByteArray): Boolean {
        val handle = tableHandle(table)

        // Fast negative search using Bloom filter
        // TODO: reduce overhead of creating direct ByteBuffer
        // val buffer = ByteBuffer.allocateDirect(key.size).rewind()
        // buffer.put(key)
        // if (!db.keyMayExist(handle, buffer)) return false
        return db.get(handle, key) != null
    }

    override fun get(table: String, key: ByteArray): ByteArray {
        val handle = tableHandle(table)
        return db.get(handle, key)
    }

    override fun put(table: String, key: ByteArray, value: ByteArray) {
        val handle = tableHandle(table)
        db.put(handle, key, value)
    }

    fun writeBatch(table: String, addedEntries: Map<ByteArray, ByteArray>, deletedEntries: Iterable<ByteArray>) {
        val handle = tableHandle(table)
        val updates = WriteBatch()
        addedEntries.forEach { updates.put(handle, it.key, it.value) }
        deletedEntries.forEach { updates.delete(handle, it) }
        // TODO: configure writeOptions
        WriteOptions().use {
            db.write(it, updates)
        }
        logger.info("Put {} records into table '{}'", addedEntries.size, table)
        logger.info("Delete {} records from table '{}'", deletedEntries.count(), table)
    }

    fun truncate(table: String) {
        logger.info("Truncating table '{}'!", table)
        dropTableIfExists(table)
        createTableIfNeeded(table)
    }
}
