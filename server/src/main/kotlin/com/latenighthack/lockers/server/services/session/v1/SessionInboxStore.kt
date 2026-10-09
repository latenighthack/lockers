package com.latenighthack.lockers.server.services.session.v1

import com.latenighthack.ktstore.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import com.latenighthack.lockers.server.ProtocolValidation
import com.latenighthack.ktcrypto.SHA256
import com.latenighthack.ktcrypto.digest
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.ServerResourceLimits
import com.latenighthack.lockers.server.ResourceLimitException

internal fun ServerSessionEvent.legacyClientEvent() = Event {
    eventId { rawValue = this@legacyClientEvent.eventId!!.rawValue }
    roomId { rawValue = this@legacyClientEvent.roomId!!.rawValue }
    roomSequence = this@legacyClientEvent.roomSequence
    if (encodedLocker.isNotEmpty()) locker = IdentifiedLocker.fromByteArray(encodedLocker)
    if (encodedPayload.isNotEmpty()) notification { payload { rawValue = encodedPayload } }
}

interface SessionInboxStore {
    /** Finite replay, bounded by both record count and encoded envelope size. */
    fun clientEventPages(sessionId: ServerSessionId, pageSize: Int = 64): Flow<List<Event>> =
        throw UnsupportedOperationException("Inbox extension requires bounded replay pages")
    /** Returns only newly accepted events. ACK must retain durable receipt identities. */
    suspend fun acceptClientEvents(events: List<Pair<ServerSessionEvent, Event>>): List<Pair<ServerSessionEvent, Event>> =
        throw UnsupportedOperationException("Inbox extension requires durable delivery receipts")
    suspend fun saveClientEvents(events: List<Pair<ServerSessionEvent, Event>>) {
        require(events.all { it.second.notification?.push == null }) { "Inbox extension must persist full notification metadata" }
        saveEvents(events.map { it.first })
    }
    suspend fun isPending(eventId: ServerEventId, sessionId: ServerSessionId): Boolean =
        throw UnsupportedOperationException("Inbox extension requires bounded pending identity lookup")
    suspend fun clientEvent(row: ServerSessionEvent): Event = row.legacyClientEvent()
    suspend fun getAllClientEvents(sessionId: ServerSessionId): List<Event> =
        getAllEvents(sessionId).sortedBy { it.roomSequence }.map { clientEvent(it) }
    suspend fun deleteAllEvents(sessionId: ServerSessionId) {
        deleteEvents(getAllEvents(sessionId).mapNotNull { it.eventId }, sessionId)
    }
    suspend fun saveEvents(events: List<ServerSessionEvent>) { events.forEach { saveEvent(it) } }
    suspend fun deleteEvents(eventIds: List<ServerEventId>, sessionId: ServerSessionId) { eventIds.forEach { deleteEvent(it, sessionId) } }
    suspend fun saveEvent(event: ServerSessionEvent)

    suspend fun getAllEvents(sessionId: ServerSessionId): List<ServerSessionEvent>

    suspend fun deleteEvent(eventId: ServerEventId, sessionId: ServerSessionId)
}

class SessionInboxStoreImpl(private val database: Database, private val limits: ServerResourceLimits = ServerResourceLimits(),
    private val clock: () -> Long = System::currentTimeMillis): SessionInboxStore, Store<ServerSessionEvent>(database, SessionInboxStoreDefinitionV2) {
    private val sessionIdKey = SessionInboxStoreDefinitionV2.sessionIdKey
    private val eventIdKey = SessionInboxStoreDefinitionV2.eventIdKey
    private val sessionIdEventIdKey = SessionInboxStoreDefinitionV2.sessionIdEventIdKey

    private class MetadataStore(database: Database) : Store<ServerSessionEvent>(database, SessionInboxMetadataDefinitionV2) {
        suspend fun saveRows(rows: List<ServerSessionEvent>) = saveAll(rows)
        suspend fun find(relation: StoreRelation) = get(relation)
        suspend fun remove(relation: StoreRelation) = delete(relation)
        suspend fun removeMany(relations: List<StoreRelation>) = deleteMany(relations)
    }
    private val metadata = MetadataStore(database)
    private fun metadataRelation(eventId: ServerEventId, sessionId: ServerSessionId) =
        SessionInboxMetadataDefinitionV2.primary.eq(listOf(
            BoundStoreKey.SerializedKey(SessionInboxMetadataDefinitionV2.sessionIdKey.name.value, sessionId.toByteArray()),
            BoundStoreKey.SerializedKey(SessionInboxMetadataDefinitionV2.eventIdKey.name.value, eventId.toByteArray()),
        ))
    private fun rowRelation(eventId: ServerEventId, sessionId: ServerSessionId) = sessionIdEventIdKey.eq(listOf(
        BoundStoreKey.SerializedKey(sessionIdKey.name.value, sessionId.toByteArray()),
        BoundStoreKey.SerializedKey(eventIdKey.name.value, eventId.toByteArray()),
    ))

    private class ReceiptStore(database: Database) : Store<InboxDeliveryReceipt>(database, InboxDeliveryReceiptDefinitionV2) {
        suspend fun find(sid: ByteArray, event: ByteArray) = get(InboxDeliveryReceiptDefinitionV2.identity.eq(inboxReceiptKey(sid, event)))
        suspend fun putAll(rows: List<InboxDeliveryReceipt>) = saveAll(rows)
    }
    private val receipts = ReceiptStore(database)
    private suspend fun validateReceiptAdmission(rows: List<InboxDeliveryReceipt>) {
        if (rows.isEmpty()) return
        val definition = InboxDeliveryReceiptDefinitionV2
        val count = database.count(definition.storeName, definition.identity.query(1))
        if (count + rows.size > limits.maxInboxReceipts) throw ResourceLimitException("Global inbox receipt capacity exhausted")
        for ((sid, added) in rows.groupBy { it.session.toList() }) {
            val id = sid.toByteArray()
            val existing = database.count(definition.storeName, definition.session.query(1, lower = id, upper = id))
            if (existing + added.size > limits.maxInboxReceiptsPerSession) throw ResourceLimitException("Session receipt capacity exhausted")
        }
    }
    override suspend fun saveClientEvents(events: List<Pair<ServerSessionEvent, Event>>) { acceptClientEvents(events) }
    override suspend fun acceptClientEvents(events: List<Pair<ServerSessionEvent, Event>>): List<Pair<ServerSessionEvent, Event>> {
        require(events.size <= 1024)
        // Freeze the bytes before waiting for the database owner: digests and persistence
        // must describe the same event even when an extension retains mutable arrays.
        val frozen = events.map { (row, event) -> ServerSessionEvent.fromByteArray(row.toByteArray()) to Event.fromByteArray(event.toByteArray()) }
        return database.transaction("inbox-metadata") {
            val now = clock()
            val definition = InboxDeliveryReceiptDefinitionV2
            database.deleteBatch(definition.storeName, definition.expires.query(256, upper = now, upperInclusive = false))
            val unique = linkedMapOf<Pair<List<Byte>, List<Byte>>, Pair<ServerSessionEvent, Event>>()
            for (pair in frozen) {
                val row = pair.first; val event = pair.second
                val sid = requireNotNull(row.sessionId).rawValue; val id = requireNotNull(row.eventId).rawValue
                require(sid.size in 1..128 && id.size in 1..128 && event.eventId?.rawValue.contentEquals(id))
                val identity = sid.toList() to id.toList()
                val prior = unique.putIfAbsent(identity, pair)
                if (prior != null && !prior.second.toByteArray().contentEquals(event.toByteArray())) throw EventIdentityReuseException()
            }
            val accepted = mutableListOf<Pair<ServerSessionEvent, Event>>()
            val backfills = mutableListOf<Pair<ServerSessionEvent, Event>>()
            val receiptRows = mutableListOf<InboxDeliveryReceipt>()
            for ((row, event) in unique.values) {
                val sid = requireNotNull(row.sessionId); val id = requireNotNull(row.eventId)
                val digest = SHA256.digest(event.toByteArray())
                val receipt = receipts.find(sid.rawValue, id.rawValue)
                if (receipt != null) {
                    if (!receipt.digest.contentEquals(digest)) throw EventIdentityReuseException()
                    continue
                }
                val current = get(rowRelation(id, sid))
                if (current != null) {
                    val full = metadata.find(metadataRelation(id, sid))
                    if (full != null) {
                        if (!full.encodedPayload.contentEquals(event.toByteArray())) throw EventIdentityReuseException()
                    } else if (current.roomSequence != row.roomSequence || !current.encodedLocker.contentEquals(row.encodedLocker) ||
                        !current.encodedPayload.contentEquals(row.encodedPayload) || !current.roomId?.rawValue.contentEquals(row.roomId?.rawValue))
                        throw EventIdentityReuseException()
                    backfills.add(row to event)
                } else accepted.add(row to event)
                receiptRows.add(InboxDeliveryReceipt(sid.rawValue, id.rawValue, digest, now + INBOX_RECEIPT_RETENTION_MILLIS))
            }
            validateReceiptAdmission(receiptRows)
            validateAdmission(accepted.map { it.first })
            saveAll(accepted.map { it.first })
            metadata.saveRows((accepted + backfills).map { (row, event) -> row.copy(encodedPayload = event.toByteArray(), encodedLocker = byteArrayOf()) })
            receipts.putAll(receiptRows)
            accepted
        }
    }
    override suspend fun isPending(eventId: ServerEventId, sessionId: ServerSessionId) = get(rowRelation(eventId, sessionId)) != null
    override suspend fun clientEvent(row: ServerSessionEvent): Event =
        metadata.find(metadataRelation(requireNotNull(row.eventId), requireNotNull(row.sessionId)))
            ?.let { Event.fromByteArray(it.encodedPayload) } ?: row.legacyClientEvent()

    private suspend fun validateAdmission(events: List<ServerSessionEvent>) {
        val additions = events.distinctBy { requireNotNull(it.sessionId) to requireNotNull(it.eventId) }
            .filter { get(rowRelation(requireNotNull(it.eventId), requireNotNull(it.sessionId))) == null }
        if (additions.isEmpty()) return
        val total = database.count(SessionInboxStoreDefinitionV2.storeName, sessionIdKey.query(1))
        if (total + additions.size > limits.maxInboxEvents) throw ResourceLimitException("Global inbox capacity exhausted")
        for ((session, rows) in additions.groupBy { requireNotNull(it.sessionId) }) {
            val count = database.count(SessionInboxStoreDefinitionV2.storeName, sessionIdKey.query(1,
                lower = session.toByteArray(), upper = session.toByteArray()))
            if (count + rows.size > limits.maxInboxEventsPerSession) throw ResourceLimitException("Session inbox capacity exhausted")
        }
    }
    override fun clientEventPages(sessionId: ServerSessionId, pageSize: Int): Flow<List<Event>> = flow {
        require(pageSize in 1..64)
        val definition = SessionInboxStoreDefinitionV2
        val prefix = inboxSessionPrefix(sessionId.rawValue)
        // Lexicographic successor excludes every other session even for all-FF IDs.
        val exclusive = prefix.copyOf().let { bytes ->
            val last = bytes.indexOfLast { (it.toInt() and 255) != 255 }
            bytes[last] = (bytes[last] + 1).toByte()
            bytes.copyOf(last + 1)
        }
        val maximum = database.transaction("inbox-metadata") {
            database.query(definition.storeName, definition.replay.query(1, lower = prefix, upper = exclusive,
                upperInclusive = false, direction = SortDirection.DESCENDING)).records.firstOrNull()?.let {
                inboxReplayKey(when (it) { is ServerSessionEvent -> it; is ByteArray -> definition.decode(it); else -> error("Invalid inbox row") })
            }
        } ?: return@flow
        var after: LocalContinuation? = null
        do {
            val (page, events) = database.transaction("inbox-metadata") {
                val page = database.query(definition.storeName, definition.replay.query(pageSize,
                    lower = prefix, upper = maximum, after = after))
                page to page.records.map { clientEvent(when (it) { is ServerSessionEvent -> it; is ByteArray -> definition.decode(it); else -> error("Invalid inbox row") }) }
            }
            // Release the database owner before suspending on the downstream socket.
            var chunk = mutableListOf<Event>(); var bytes = 1024
            for (event in events) {
                val size = event.toByteArray().size + 16
                if (size + 1024 > ProtocolValidation.MAX_ENVELOPE_BYTES) throw ResourceLimitException("Inbox event exceeds envelope capacity")
                if (bytes + size > ProtocolValidation.MAX_ENVELOPE_BYTES && chunk.isNotEmpty()) {
                    emit(chunk.toList()); chunk = mutableListOf(); bytes = 1024
                }
                chunk.add(event); bytes += size
            }
            if (chunk.isNotEmpty()) emit(chunk.toList())
            after = page.continuation
        } while (after != null)
    }
    override suspend fun getAllClientEvents(sessionId: ServerSessionId): List<Event> = database.transaction("inbox-metadata") {
        getAllEvents(sessionId).sortedBy { it.roomSequence }.map { clientEvent(it) }
    }
    override suspend fun saveEvent(event: ServerSessionEvent) = saveEvents(listOf(event))
    override suspend fun saveEvents(events: List<ServerSessionEvent>) { acceptClientEvents(events.map { it to it.legacyClientEvent() }) }
    override suspend fun deleteEvents(eventIds: List<ServerEventId>, sessionId: ServerSessionId) = database.transaction("inbox-metadata") {
        deleteMany(eventIds.map { rowRelation(it, sessionId) })
        metadata.removeMany(eventIds.map { metadataRelation(it, sessionId) })
    }
    override suspend fun deleteAllEvents(sessionId: ServerSessionId) = database.transaction("inbox-metadata") {
        delete(sessionIdKey.eq(sessionId.toByteArray()))
        metadata.remove(SessionInboxMetadataDefinitionV2.sessionIdKey.eq(sessionId.toByteArray()))
    }
    override suspend fun getAllEvents(sessionId: ServerSessionId) = getAll(sessionIdKey.eq(sessionId.toByteArray()))
    override suspend fun deleteEvent(eventId: ServerEventId, sessionId: ServerSessionId) = deleteEvents(listOf(eventId), sessionId)
}
