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
}
