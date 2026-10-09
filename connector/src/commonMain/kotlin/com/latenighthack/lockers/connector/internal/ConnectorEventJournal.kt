package com.latenighthack.lockers.connector.internal

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.connector.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*

/** Stable local cursor. Keep it only after the application has handled [payload]. */
data class ConnectorJournalEntry(val cursor: Long, val kind: Int, val payload: ByteArray, val identity: ByteArray)
internal fun longBytes(value: Long) = ByteArray(8) { (value ushr (56 - 8 * it)).toByte() }
internal fun bytesLong(bytes: ByteArray): Long { require(bytes.size == 8); return bytes.fold(0L) { acc, byte -> (acc shl 8) or (byte.toLong() and 255) } }
private val ConnectorJournalEntry.cursorKey: ByteArray get() = longBytes(cursor)
private fun encodeEntry(entry: ConnectorJournalEntry) = encodeFrames(longBytes(entry.cursor), longBytes(entry.kind.toLong()), entry.payload, entry.identity)
private fun decodeEntry(bytes: ByteArray): ConnectorJournalEntry {
    val fields = decodeFrames(bytes); require(fields.size == 4)
    return ConnectorJournalEntry(bytesLong(fields[0]), bytesLong(fields[1]).toInt(), fields[2], fields[3])
}
object ConnectorEventJournalDefinitionV1 : StoreDefinition<ConnectorJournalEntry>(
    StoreName("connector_events"), "ConnectorEvent-frames-v1", ::decodeEntry, ::encodeEntry,
) {
    val cursor = bytesIndex(IndexName("cursor"), ConnectorJournalEntry::cursorKey, "unsigned-big-endian-i64-v1").also { primaryKey(it) }
    val identity = bytesIndex(IndexName("event_identity"), ConnectorJournalEntry::identity, "framed-identity-v1")
}

internal class ConnectorEventJournal(private val database: Database, private val policy: ConnectorRetentionPolicy = ConnectorRetentionPolicy()) : Store<ConnectorJournalEntry>(database, ConnectorEventJournalDefinitionV1) {
    private val revision = MutableStateFlow(0L)
    private val retention = JournalRetentionStore(database)
    suspend fun head(): Long { prepare(); return get(ConnectorEventJournalDefinitionV1.cursor.eq(longBytes(0)))?.payload?.let(::bytesLong) ?: 0 }
    suspend fun append(kind: Int, payload: ByteArray, identity: ByteArray): ConnectorJournalEntry {
        prepare()
        return database.transaction("connector-accept") {
            get(ConnectorEventJournalDefinitionV1.identity.eq(identity)) ?: run {
                val previous = head()
                val retained = retention.current()
                val bytes = payload.size.toLong() + identity.size + 16
                if (previous - retained.floor >= policy.maxAcceptedEvents || bytes > policy.maxAcceptedEventBytes - retained.retainedBytes) throw ConnectorRetentionExceededException("Accepted event journal is full; advance the application cursor retention watermark")
                val cursor = previous + 1
                check(cursor > 0) { "Connector event cursor exhausted" }
                val entry = ConnectorJournalEntry(cursor, kind, payload.copyOf(), identity.copyOf())
                save(entry)
                save(ConnectorJournalEntry(0, 0, longBytes(cursor), byteArrayOf()))
                retention.update(retained.copy(retainedBytes = retained.retainedBytes + bytes))
                revision.value = cursor
                entry
            }
        }
    }
    private suspend fun page(after: Long): List<ConnectorJournalEntry> {
        prepare()
        val floor = retention.floor()
        if (after < floor) throw ConnectorCursorExpiredException(after, floor)
        return database.transaction(setOf(ConnectorEventJournalDefinitionV1.storeName), TransactionMode.READ_ONLY) {
            query(ConnectorEventJournalDefinitionV1.storeName, IndexedQuery(
                ConnectorEventJournalDefinitionV1.cursor.key, 64,
                lower = QueryBound(BoundStoreKey.SerializedKey(ConnectorEventJournalDefinitionV1.cursor.name.value, longBytes(after)), inclusive = false),
            )).records.map { if (it is ConnectorJournalEntry) it else ConnectorEventJournalDefinitionV1.decode(it as ByteArray) }
        }
    }
    /** Only call after every independent consumer has durably handled all events through this cursor. */
    suspend fun pruneThrough(cursor: Long) {
        require(cursor >= 0)
        prepare(); retention.prepare()
        while (true) {
            val removed = database.transaction("connector-accept") {
                val floor = retention.floor()
                require(cursor <= head()) { "Cannot prune beyond accepted journal head" }
                if (cursor <= floor) return@transaction 0
                val page = database.query(ConnectorEventJournalDefinitionV1.storeName, IndexedQuery(ConnectorEventJournalDefinitionV1.cursor.key, 64,
                    lower = QueryBound(BoundStoreKey.SerializedKey(ConnectorEventJournalDefinitionV1.cursor.name.value, longBytes(floor)), false),
                    upper = QueryBound(BoundStoreKey.SerializedKey(ConnectorEventJournalDefinitionV1.cursor.name.value, longBytes(cursor)), true)))
                val last = page.records.lastOrNull()?.let { if (it is ConnectorJournalEntry) it.cursor else ConnectorEventJournalDefinitionV1.decode(it as ByteArray).cursor } ?: cursor
                val removed = database.deleteBatch(ConnectorEventJournalDefinitionV1.storeName, IndexedQuery(ConnectorEventJournalDefinitionV1.cursor.key, 64,
                    lower = QueryBound(BoundStoreKey.SerializedKey(ConnectorEventJournalDefinitionV1.cursor.name.value, longBytes(floor)), false),
                    upper = QueryBound(BoundStoreKey.SerializedKey(ConnectorEventJournalDefinitionV1.cursor.name.value, longBytes(last)), true)))
                val removedBytes = page.records.sumOf { val entry = if (it is ConnectorJournalEntry) it else ConnectorEventJournalDefinitionV1.decode(it as ByteArray); entry.payload.size.toLong() + entry.identity.size + 16 }
                retention.update(JournalRetention(if (removed < 64) cursor else last, (retention.current().retainedBytes - removedBytes).coerceAtLeast(0)))
                removed
            }
            if (removed < 64) return
        }
    }
    fun after(cursor: Long): Flow<ConnectorJournalEntry> = flow {
        require(cursor >= 0)
        var consumed = cursor
        while (currentCoroutineContext().isActive) {
            val hint = revision.value
            val entries = page(consumed)
            entries.forEach { emit(it); consumed = it.cursor }
            if (entries.size < 64) withTimeoutOrNull(1_000) { revision.first { it != hint } }
        }
    }
    fun live(): Flow<ConnectorJournalEntry> = flow { emitAll(after(head())) }
}
