package com.latenighthack.lockers.connector.internal

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.connector.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

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
    fun after(cursor: Long): Flow<ConnectorJournalEntry> { require(cursor >= 0); return read { cursor } }
    fun live(): Flow<ConnectorJournalEntry> = read { head() }

    /**
     * Entries of [kind] after [consumer]'s saved cursor, resuming where its last run stopped. The
     * cursor is saved once the collector has handled an entry, every [SAVE_INTERVAL] entries and
     * whenever the reader catches up, so a restart repeats at most those entries and never skips
     * one. Catching up also prunes the journal behind every saved cursor and every collecting reader.
     */
    fun durable(consumer: String, kind: Int): Flow<ConnectorJournalEntry> = flow {
        val key = JournalRetentionStore.consumerKey(consumer)
        var saved = 0L
        emitAll(read(
            start = {
                prepare()
                val floor = retention.floor()
                // A new consumer pins the journal where it starts, before anything can prune past it.
                val resumed = retention.cursor(key)?.coerceAtLeast(floor)
                    ?: floor.also { retention.saveCursor(key, it) }
                resumed.also { saved = it }
            },
            handled = { entry ->
                if (entry.cursor - saved >= SAVE_INTERVAL) {
                    retention.saveCursor(key, entry.cursor)
                    saved = entry.cursor
                }
            },
            caughtUp = { consumed ->
                if (consumed != saved) { retention.saveCursor(key, consumed); saved = consumed }
                pruneBehindReaders()
            },
            // A collector that stops keeps what it handled; only a killed process repeats entries.
            stopped = { consumed -> if (consumed > saved) { retention.saveCursor(key, consumed); saved = consumed } },
        ).filter { it.kind == kind })
    }

    /**
     * Pages entries after [start], registered with [JournalReaders] while collecting so pruning
     * never removes an entry a reader hasn't reached. A reader registers at cursor 0 until its
     * start is known, which holds off pruning for that moment.
     */
    private fun read(
        handled: suspend (ConnectorJournalEntry) -> Unit = {},
        caughtUp: suspend (Long) -> Unit = {},
        stopped: suspend (Long) -> Unit = {},
        start: suspend () -> Long,
    ): Flow<ConnectorJournalEntry> = flow {
        val reader = JournalReaders.open(database)
        var consumed = -1L
        try {
            consumed = start()
            JournalReaders.advance(database, reader, consumed)
            while (true) {
                currentCoroutineContext().ensureActive()
                val hint = revision.value
                val entries = page(consumed)
                entries.forEach { emit(it); consumed = it.cursor; handled(it) }
                JournalReaders.advance(database, reader, consumed)
                if (entries.size < 64) {
                    caughtUp(consumed)
                    withTimeoutOrNull(1_000) { revision.first { it != hint } }
                }
            }
        } finally {
            withContext(NonCancellable) {
                // Best effort: a closed database can't save, and the next run repeats from the last save.
                try {
                    if (consumed >= 0) runCatching { stopped(consumed) }
                } finally { JournalReaders.close(database, reader) }
            }
        }
    }

    private suspend fun pruneBehindReaders() {
        val floor = retention.floor()
        val saved = retention.consumerCursors().minOrNull()
        val bound = listOfNotNull(saved, JournalReaders.lowest(database)).minOrNull() ?: return
        if (bound - floor >= PRUNE_BATCH) pruneThrough(minOf(bound, head()))
    }

    private companion object {
        const val SAVE_INTERVAL = 64L
        const val PRUNE_BATCH = 64L
    }
}

/**
 * Each collecting journal reader's last handled cursor, per database. The locker and session
 * journals share one table, so pruning from either must respect both journals' readers.
 */
private object JournalReaders {
    private val mutex = Mutex()
    private val positions = HashMap<Database, HashMap<Long, Long>>()
    private var next = 0L
    suspend fun open(database: Database): Long = mutex.withLock {
        (next++).also { positions.getOrPut(database) { HashMap() }[it] = 0L }
    }
    suspend fun advance(database: Database, reader: Long, cursor: Long) = mutex.withLock {
        positions[database]?.let { if (reader in it) it[reader] = cursor }
    }
    suspend fun close(database: Database, reader: Long) = mutex.withLock {
        positions[database]?.let { it.remove(reader); if (it.isEmpty()) positions.remove(database) }
    }
    suspend fun lowest(database: Database): Long? = mutex.withLock { positions[database]?.values?.minOrNull() }
}
