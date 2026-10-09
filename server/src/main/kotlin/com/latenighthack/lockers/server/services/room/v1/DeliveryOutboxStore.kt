package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.server.storage.v2.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/** Source writes, sequences, and indexed durable delivery commit on the same storage connection. */
class DeliveryOutboxStore(private val delegate: Database, private val prefix: String = "delivery",
    private val policy: OutboxPolicy = OutboxPolicy(), private val clock: () -> Long = System::currentTimeMillis,
) : Store<ServerDeliveryIntent>(delegate, DeliveryOutboxStoreDefinitionV1(prefix)) {
    private val available = kotlinx.coroutines.channels.Channel<Unit>(kotlinx.coroutines.channels.Channel.CONFLATED)
    suspend fun awaitWork() { kotlinx.coroutines.withTimeoutOrNull(250) { available.receive() } }
    private val definition = DeliveryOutboxStoreDefinitionV1(prefix)
    private val id = definition.id
    private val entriesDefinition = OutboxEntriesDefinitionV2(prefix)
    private val headsDefinition = OutboxHeadsDefinitionV2(prefix)
    private class Entries(db: Database, val definition: OutboxEntriesDefinitionV2) : Store<ServerOutboxEntry>(db, definition) {
        suspend fun find(id: ByteArray) = get(definition.id.eq(id))
        suspend fun put(row: ServerOutboxEntry) = save(row)
        suspend fun remove(id: ByteArray) = delete(definition.id.eq(id))
    }
    private class Heads(db: Database, val definition: OutboxHeadsDefinitionV2) : Store<ServerOutboxHead>(db, definition) {
        suspend fun put(row: ServerOutboxHead) = save(row)
        suspend fun remove(room: ByteArray) = delete(definition.room.eq(room))
    }
    private val entries = Entries(delegate, entriesDefinition)
    private val heads = Heads(delegate, headsDefinition)
    private val sequences = RoomSequences(delegate, prefix)
    private val receipts = WriteReceipts(delegate, prefix)
    private val migration = Mutex()
    private var indexed = false
    private var migrationAfter: LocalContinuation? = null
    suspend fun prepareStores() { prepare(); sequences.prepare(); receipts.prepare(); entries.prepare(); heads.prepare() }
    suspend fun receipt(room: RoomId, id: ByteArray) = receipts.find(ServerRoomId(room.rawValue), id)
    suspend fun saveReceipt(receipt: ServerWriteReceipt) = receipts.put(receipt)
    suspend fun <T> atomic(block: suspend () -> T): T = delegate.transaction(LOCK, block)
    suspend fun <T> atomic(roomId: RoomId, block: suspend () -> T): T = delegate.transaction(roomMutationKey(ServerRoomId(roomId.rawValue)), block)
    private suspend fun <T> mutateRoom(roomId: RoomId, block: suspend () -> T): T =
        atomic(roomId) { delegate.transaction("lockers.$prefix-outbox-quota", block) }
    suspend fun watermark(roomId: RoomId): Long = sequences.value(ServerRoomId(roomId.rawValue))
    private fun decodeEntry(raw: Any) = if (raw is ServerOutboxEntry) raw else ServerOutboxEntry.fromByteArray(raw as ByteArray)
    private fun decodeIntent(raw: Any) = if (raw is ServerDeliveryIntent) raw else ServerDeliveryIntent.fromByteArray(raw as ByteArray)
    private fun roomQuery(index: TypedIndex<ServerOutboxEntry, ByteArray>, room: ByteArray, limit: Int) = index.query(limit,
        lower = outboxRoomPrefix(room), upper = outboxRoomPrefix(room) + ByteArray(8) { -1 })
    private fun metadata(intent: ServerDeliveryIntent): ServerOutboxEntry {
        val created = clock().coerceAtLeast(1)
        check(created <= Long.MAX_VALUE - policy.retryWindowMs) { "Outbox deadline exhausted" }
        return ServerOutboxEntry(eventId = requireNotNull(intent.eventId).rawValue, roomId = requireNotNull(intent.roomId).rawValue,
            sequence = intent.roomSequence, createdAt = created, deadline = created + policy.retryWindowMs)
    }
    private suspend fun active(room: ByteArray, limit: Int): List<ServerOutboxEntry> =
        delegate.query(entriesDefinition.storeName, roomQuery(entriesDefinition.active, room, limit)).records.map(::decodeEntry)

    /** Canonical rows are rechecked; a migration snapshot cannot resurrect accepted delivery. */
    private suspend fun refreshHead(room: ByteArray) {
        repeat(128) {
            val first = active(room, 1).firstOrNull()
            if (first == null) { heads.remove(room); return }
            val canonical = get(id.eq(ServerEventId(first.eventId).toByteArray()))
            if (canonical == null) { entries.remove(first.eventId); return@repeat }
            heads.put(ServerOutboxHead(roomId = room, eventId = first.eventId, sequence = first.sequence,
                dueAt = maxOf(canonical.leaseUntil, canonical.retryAfter)))
            return
        }
        error("Too many orphan outbox index records")
    }

    /** Bounded adoption transactions complete before claims, so legacy prefixes cannot be skipped. */
    suspend fun initialize() = migration.withLock {
        while (!indexed) {
            val page = delegate.query(definition.storeName, id.query(128, after = migrationAfter))
            page.records.map(::decodeIntent).groupBy { requireNotNull(it.roomId) }.forEach { (room, rows) ->
                mutateRoom(RoomId(room.rawValue)) {
                    for (snapshot in rows) {
                        val event = requireNotNull(snapshot.eventId)
                        val current = get(id.eq(event.toByteArray())) ?: continue
                        if (entries.find(event.rawValue) == null) entries.put(metadata(current))
                    }
                    refreshHead(room.rawValue)
                }
            }
            migrationAfter = page.continuation
            indexed = migrationAfter == null
        }
    }

    suspend fun <T> commit(roomId: RoomId, recipients: List<SessionId>, events: List<Event>, mutation: suspend () -> T): T {
        val result = mutateRoom(roomId) {
            val result = mutation()
            if (events.isEmpty()) return@mutateRoom result
            check(events.map { requireNotNull(it.eventId).rawValue.toList() }.toSet().size == events.size) { "Duplicate event identities in one commit" }
            val canonicalRoom = ServerRoomId(roomId.rawValue).toByteArray()
            val count = delegate.count(definition.storeName, definition.room.query(1, lower = canonicalRoom, upper = canonicalRoom)) +
                delegate.count(entriesDefinition.storeName, roomQuery(entriesDefinition.parkedRoom, roomId.rawValue, 1))
            check(count <= policy.retainedPerRoom - events.size.toLong()) { "Room delivery capacity exceeded" }
            val retained = delegate.count(definition.storeName, id.query(1)) +
                delegate.count(entriesDefinition.storeName, entriesDefinition.parked.query(1)) +
                delegate.count(entriesDefinition.storeName, entriesDefinition.completed.query(1))
            check(retained <= policy.retainedGlobal - events.size.toLong()) { "Global delivery capacity exceeded" }
            val serverRoom = ServerRoomId(roomId.rawValue)
            var sequence = sequences.value(serverRoom)
            check(sequence >= 0 && events.size.toLong() <= Long.MAX_VALUE - sequence) { "Outbox sequence exhausted" }
            val intents = events.map { event ->
                val eventId = requireNotNull(event.eventId)
                check(entries.find(eventId.rawValue) == null) { "Delivery event identity reused" }
                ServerDeliveryIntent(eventId = ServerEventId(eventId.rawValue), roomId = serverRoom, roomSequence = ++sequence,
                    encodedEvent = event.copy(roomSequence = sequence).toByteArray(),
                    pendingSessions = recipients.distinct().map { ServerSessionId(it.rawValue) })
            }
            sequences.set(serverRoom, sequence)
            saveAll(intents)
            intents.forEach { entries.put(metadata(it)) }
            refreshHead(roomId.rawValue)
            result
        }
        if (events.isNotEmpty()) available.trySend(Unit)
        return result
    }

    private suspend fun park(intent: ServerDeliveryIntent, reason: String) {
        val key = requireNotNull(intent.eventId).rawValue
        val old = entries.find(key) ?: metadata(intent)
        entries.put(old.copy(parkedReason = reason, parkedIntent = intent.copy(leaseOwner = "", leaseUntil = 0).toByteArray()))
        delete(id.eq(requireNotNull(intent.eventId).toByteArray()))
    }

    suspend fun claim(owner: String, now: Long, leaseMs: Long = 30_000, limit: Int = 4, eventsPerRoom: Int = 1): List<ServerDeliveryIntent> {
        require(limit in 1..64 && eventsPerRoom in 1..64 && leaseMs in 1..30_000 && now >= 0 && now <= Long.MAX_VALUE - leaseMs)
        initialize()
        pruneResolved(now)
        val candidates = delegate.query(headsDefinition.storeName, headsDefinition.due.query(limit,
            lower = OrderedKeyEncoding.long(0), upper = OrderedKeyEncoding.long(now))).records
            .map { if (it is ServerOutboxHead) it else ServerOutboxHead.fromByteArray(it as ByteArray) }
        return candidates.flatMap { head -> mutateRoom(RoomId(head.roomId)) {
            val claimed = mutableListOf<ServerDeliveryIntent>()
            for (entry in active(head.roomId, eventsPerRoom)) {
                val current = get(id.eq(ServerEventId(entry.eventId).toByteArray()))
                if (current == null) { entries.remove(entry.eventId); continue }
                if (current.leaseUntil > now || current.retryAfter > now) break
                if (now >= entry.deadline) { park(current, "Automatic retry deadline exceeded"); continue }
                if (current.attempts == Int.MAX_VALUE) { park(current, "Attempt counter exhausted"); continue }
                val next = current.copy(leaseOwner = "$owner:${java.util.UUID.randomUUID()}", leaseUntil = now + leaseMs, attempts = current.attempts + 1)
                save(next); claimed.add(next)
            }
            refreshHead(head.roomId)
            claimed
        } }
    }

    suspend fun accepted(intent: ServerDeliveryIntent, sessions: List<SessionId>) = mutateRoom(RoomId(requireNotNull(intent.roomId).rawValue)) {
        val current = get(id.eq(requireNotNull(intent.eventId).toByteArray())) ?: return@mutateRoom
        if (current.leaseOwner != intent.leaseOwner) return@mutateRoom
        val accepted = sessions.map { ServerSessionId(it.rawValue) }.toSet()
        val pending = current.pendingSessions.filterNot { it in accepted }
        if (pending.isEmpty()) {
            delete(id.eq(requireNotNull(current.eventId).toByteArray()))
            entries.remove(requireNotNull(current.eventId).rawValue)
        } else save(current.copy(pendingSessions = pending))
        refreshHead(requireNotNull(current.roomId).rawValue)
    }

    suspend fun renew(intents: List<ServerDeliveryIntent>, now: Long, leaseMs: Long = 30_000) {
        require(leaseMs in 1..30_000 && now <= Long.MAX_VALUE - leaseMs)
        intents.groupBy { requireNotNull(it.roomId) }.forEach { (room, rows) -> mutateRoom(RoomId(room.rawValue)) {
            for (intent in rows) {
                val current = get(id.eq(requireNotNull(intent.eventId).toByteArray())) ?: continue
                if (current.leaseOwner == intent.leaseOwner) save(current.copy(leaseUntil = now + leaseMs))
            }
            refreshHead(room.rawValue)
        } }
    }
    suspend fun retry(intent: ServerDeliveryIntent, now: Long) { retryIfPending(intent, now) }
    suspend fun retryIfPending(intent: ServerDeliveryIntent, now: Long): Boolean = mutateRoom(RoomId(requireNotNull(intent.roomId).rawValue)) {
        val current = get(id.eq(requireNotNull(intent.eventId).toByteArray())) ?: return@mutateRoom false
        if (current.leaseOwner != intent.leaseOwner) return@mutateRoom false
        val delay = minOf(30_000L, 100L shl minOf(current.attempts, 8))
        check(now >= 0 && now <= Long.MAX_VALUE - delay)
        save(current.copy(leaseOwner = "", leaseUntil = 0, retryAfter = now + delay))
        refreshHead(requireNotNull(current.roomId).rawValue)
        true
    }

    /** Trusted operator read. Parked records never expire or disappear automatically. */
    suspend fun parked(limit: Int = 128): List<ServerOutboxEntry> {
        require(limit in 1..256)
        return delegate.query(entriesDefinition.storeName, entriesDefinition.parked.query(limit)).records.map(::decodeEntry)
    }
    /** Trusted reconciliation; reissue delivery with a fresh event identity, never replay this one. */
    suspend fun resolveParked(eventId: ByteArray): Boolean {
        val entry = entries.find(eventId) ?: return false
        return mutateRoom(RoomId(entry.roomId)) {
            val current = entries.find(eventId) ?: return@mutateRoom false
            if (current.parkedReason.isEmpty() || current.resolvedAt != 0L) return@mutateRoom false
            entries.put(current.copy(resolvedAt = clock().coerceAtLeast(1)))
            true
        }
    }
    private suspend fun pruneResolved(now: Long) {
        if (now <= policy.completedRetentionMs) return
        delegate.transaction("lockers.$prefix-outbox-quota") {
            delegate.deleteBatch(entriesDefinition.storeName, entriesDefinition.completed.query(128,
                upper = OrderedKeyEncoding.long(now - policy.completedRetentionMs)))
        }
    }
    suspend fun pendingCountLong(): Long = delegate.count(definition.storeName, id.query(1))
    suspend fun pendingCount(): Int = pendingCountLong().coerceAtMost(Int.MAX_VALUE.toLong()).toInt()

    private class WriteReceipts(delegate: Database, prefix: String) : Store<ServerWriteReceipt>(delegate, WriteReceiptsDefinitionV1(prefix)) {
        private val room = WriteReceiptsDefinitionV1(prefix).room
        private val request = WriteReceiptsDefinitionV1(prefix).request
        private val key = WriteReceiptsDefinitionV1(prefix).key
        suspend fun find(roomId: ServerRoomId, requestId: ByteArray) = get(key.eq(listOf(
            BoundStoreKey.SerializedKey(room.name.value, roomId.toByteArray()), BoundStoreKey.SerializedKey(request.name.value, requestId))))
        suspend fun put(receipt: ServerWriteReceipt) = save(receipt)
    }
    private class RoomSequences(delegate: Database, prefix: String) : Store<ServerRoomSequence>(delegate, RoomSequencesDefinitionV1(prefix)) {
        private val room = RoomSequencesDefinitionV1(prefix).room
        suspend fun value(id: ServerRoomId) = get(room.eq(id.toByteArray()))?.sequence ?: 0L
        suspend fun set(id: ServerRoomId, sequence: Long) = save(ServerRoomSequence(id, sequence))
    }
    private val LOCK = "lockers.$prefix-outbox"
}
