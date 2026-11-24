package com.latenighthack.lockers.connector.internal
import com.latenighthack.ktstore.*
data class JournalRetention(val floor: Long, val retainedBytes: Long = 0, val key: Int = 1)
object JournalRetentionDefinitionV1 : StoreDefinition<JournalRetention>(StoreName("connector_retention"), "JournalRetention-frames-v1",
    { decodeFrames(it).let { fields -> require(fields.size == 2); JournalRetention(bytesLong(fields[0]), bytesLong(fields[1])) } }, { encodeFrames(longBytes(it.floor), longBytes(it.retainedBytes)) }) {
    val key = integerIndex(IndexName("key"), JournalRetention::key).also { primaryKey(it) }
}
internal class JournalRetentionStore(database: Database) : Store<JournalRetention>(database, JournalRetentionDefinitionV1) {
    suspend fun current(): JournalRetention { prepare(); return get(JournalRetentionDefinitionV1.key.eq(1)) ?: JournalRetention(0) }
    suspend fun floor(): Long = current().floor
    suspend fun update(value: JournalRetention) { prepare(); save(value) }

    // Named consumers' saved cursors share this store with the retention row (key 1), so they need
    // no new store and clear with the journal. A cursor row is keyed from its consumer's name and
    // marks itself with retainedBytes = -key: the stored frames carry no key, and the retention
    // row's retainedBytes is never negative. Older connectors read only key 1.
    suspend fun cursor(consumerKey: Int): Long? {
        prepare()
        return get(JournalRetentionDefinitionV1.key.eq(consumerKey))?.floor
    }
    suspend fun saveCursor(consumerKey: Int, cursor: Long) {
        prepare()
        save(JournalRetention(cursor, -consumerKey.toLong(), consumerKey))
    }
    suspend fun consumerCursors(): List<Long> {
        prepare()
        return getAll().filter { it.retainedBytes < 0 }.map { it.floor }
    }

    companion object {
        /** A stable key from 2 up for [name] (FNV-1a), clear of the retention row's key 1. */
        private const val FNV_OFFSET = 0x811C9DC5.toInt()
        private const val FNV_PRIME = 0x01000193
        private const val BYTE_MASK = 0xFF
        private const val KEY_MASK = 0x3FFF_FFFF
        private const val FIRST_CONSUMER_KEY = 2

        fun consumerKey(name: String): Int {
            var hash = FNV_OFFSET
            for (byte in name.encodeToByteArray()) hash = (hash xor (byte.toInt() and BYTE_MASK)) * FNV_PRIME
            return FIRST_CONSUMER_KEY + (hash and KEY_MASK)
        }
    }
}
