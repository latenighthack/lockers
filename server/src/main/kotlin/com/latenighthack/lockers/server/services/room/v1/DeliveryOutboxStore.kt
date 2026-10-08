package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.storage.v1.*

/** Locker mutations and delivery intents use the same delegate/connection. No RPC runs here. */
class DeliveryOutboxStore(private val delegate: Database, private val prefix: String = "delivery") : Store<ServerDeliveryIntent>(delegate, DeliveryOutboxStoreDefinitionV1(prefix)) {
    private val available = kotlinx.coroutines.channels.Channel<Unit>(kotlinx.coroutines.channels.Channel.CONFLATED)
    suspend fun awaitWork() { kotlinx.coroutines.withTimeoutOrNull(250) { available.receive() } }
    private val id = DeliveryOutboxStoreDefinitionV1(prefix).id
    private val room = DeliveryOutboxStoreDefinitionV1(prefix).room
    private val sequences = RoomSequences(delegate, prefix)
    private val receipts = WriteReceipts(delegate, prefix)
    suspend fun prepareStores() { prepare(); sequences.prepare(); receipts.prepare() }
    suspend fun receipt(room: RoomId, id: ByteArray) = receipts.find(ServerRoomId(room.rawValue), id)
    suspend fun saveReceipt(receipt: ServerWriteReceipt) = receipts.put(receipt)
    suspend fun <T> atomic(block: suspend () -> T): T = delegate.transaction(LOCK, block)
    suspend fun watermark(roomId: RoomId): Long = sequences.value(ServerRoomId(roomId.rawValue))

    suspend fun <T> commit(roomId: RoomId, recipients: List<SessionId>, events: List<Event>, mutation: suspend () -> T): T {
        val committed = delegate.transaction(LOCK) {
            val result = mutation()
            if (events.isEmpty()) return@transaction result
            val serverRoom = ServerRoomId(roomId.rawValue)
            var sequence = sequences.value(serverRoom)
            val intents = events.map { event ->
                sequence++
                ServerDeliveryIntent(
                    eventId = ServerEventId(requireNotNull(event.eventId).rawValue), roomId = serverRoom,
                    roomSequence = sequence, encodedEvent = event.copy(roomSequence = sequence).toByteArray(),
                    pendingSessions = recipients.distinct().map { ServerSessionId(it.rawValue) }
                )
            }
            sequences.set(serverRoom, sequence)
            saveAll(intents)
            result
        }
        if (events.isNotEmpty()) available.trySend(Unit)
        return committed
    }

    /** Only the head of each room is claimable, including while another worker owns its lease. */
    suspend fun claim(owner: String, now: Long, leaseMs: Long = 30_000, limit: Int = 4, eventsPerRoom: Int = 1): List<ServerDeliveryIntent> =
        delegate.transaction(LOCK) {
            val rooms = getAll().groupBy { it.roomId }.values.map { rows -> rows.sortedBy { it.roomSequence } }
            val ready = rooms.filter { rows -> rows.first().let { it.leaseUntil <= now && it.retryAfter <= now } }.take(limit)
            val claimed = ready.flatMap { rows -> rows.takeWhile { it.leaseUntil <= now && it.retryAfter <= now }.take(eventsPerRoom) }
                .map { it.copy(leaseOwner = "$owner:${java.util.UUID.randomUUID()}", leaseUntil = now + leaseMs, attempts = it.attempts + 1) }
            saveAll(claimed)
            claimed
        }

    suspend fun accepted(intent: ServerDeliveryIntent, sessions: List<SessionId>) = delegate.transaction(LOCK) {
        val current = get(id.eq(requireNotNull(intent.eventId).toByteArray())) ?: return@transaction
        if (current.leaseOwner != intent.leaseOwner) return@transaction
        val accepted = sessions.map { ServerSessionId(it.rawValue) }.toSet()
        val pending = current.pendingSessions.filterNot { it in accepted }
        if (pending.isEmpty()) delete(id.eq(requireNotNull(current.eventId).toByteArray()))
        else save(current.copy(pendingSessions = pending))
    }

    suspend fun renew(intents: List<ServerDeliveryIntent>, now: Long, leaseMs: Long = 30_000) = delegate.transaction(LOCK) {
        val owners = intents.associate { it.eventId to it.leaseOwner }
        val rows = getMany(intents.map { id.eq(requireNotNull(it.eventId).toByteArray()) })
        saveAll(rows.filter { owners[it.eventId] == it.leaseOwner }.map { it.copy(leaseUntil = now + leaseMs) })
    }

    suspend fun retry(intent: ServerDeliveryIntent, now: Long) { retryIfPending(intent, now) }
    suspend fun retryIfPending(intent: ServerDeliveryIntent, now: Long): Boolean = delegate.transaction(LOCK) {
        val current = get(id.eq(requireNotNull(intent.eventId).toByteArray())) ?: return@transaction false
        if (current.leaseOwner != intent.leaseOwner) return@transaction false
        save(current.copy(leaseOwner = "", leaseUntil = 0, retryAfter = now + minOf(30_000L, 100L shl minOf(current.attempts, 8))))
        true
    }
    suspend fun pendingCountLong(): Long = delegate.transaction(setOf(DeliveryOutboxStoreDefinitionV1(prefix).storeName), TransactionMode.READ_ONLY) { count(DeliveryOutboxStoreDefinitionV1(prefix).storeName, id.query(1)) }
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
