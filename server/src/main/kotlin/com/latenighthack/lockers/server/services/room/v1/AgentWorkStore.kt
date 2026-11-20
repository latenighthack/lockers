package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.lockers.server.tools.QueueSnapshot
import com.latenighthack.lockers.server.tools.producerTraceparent
import com.latenighthack.ktstore.*
import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.storage.v2.*
import com.latenighthack.lockers.server.storage.v1.toByteArray

/** Journal leases protect status/result transitions; the room fence separately protects derived writes. */
class AgentWorkStore(private val database: Database, private val clock: () -> Long = System::currentTimeMillis,
    private val retentionMs: Long = 31L * 24 * 60 * 60 * 1000,
    private val globalCapacity: Long = 1_000_000, private val roomCapacity: Long = 4096,
) : Store<ServerAgentWork>(database, AgentWorkDefinitionV3) {
    init { require(retentionMs > 0 && globalCapacity > 0 && roomCapacity > 0) }
    private val lock = AGENT_WORK_LOCK
    private val counters = CapacityCounters(database)
    private val family = CapacityCounters.agentWork
    // Every receipt is journaled 1:1 in the same transaction (or adopted at startup), so the
    // journal counters also bound the receipts that used to be counted separately.
    private fun capacity(room: RoomId) = listOf(
        family.global { database.count(AgentWorkDefinitionV3.storeName, AgentWorkDefinitionV3.id.query(1)) },
        family.room(room.rawValue) {
            database.count(AgentWorkDefinitionV3.storeName, roomQuery(AgentWorkDefinitionV3.room, room.rawValue, 1))
        })
    /** Reads both journal counters once, rejects at the configured boundary, and reserves one row. */
    private suspend fun admit(room: RoomId) {
        val scope = capacity(room)
        val (global, inRoom) = counters.read(scope).let { it[0] to it[1] }
        check(global < globalCapacity) { "Agent journal global capacity exceeded" }
        check(inRoom < roomCapacity) { "Agent journal room capacity exceeded" }
        counters.write(scope, longArrayOf(global + 1, inRoom + 1))
    }
    private fun decode(raw: Any) = if (raw is ServerAgentWork) raw else ServerAgentWork.fromByteArray(raw as ByteArray)
    private fun roomQuery(index: TypedIndex<ServerAgentWork, ByteArray>, room: ByteArray, limit: Int) = index.query(limit,
        lower = agentRoomPrefix(room), upper = agentRoomPrefix(room) + ByteArray(72) { -1 })
    private fun key(room: RoomId, id: ByteArray) = agentRoomPrefix(room.rawValue) + id
    suspend fun find(room: RoomId, id: ByteArray): ServerAgentWork? = get(AgentWorkDefinitionV3.id.eq(key(room, id)))
    /**
     * Journals one accepted write. [receiptAbsent] records that the caller found no write receipt
     * for this identity in the same transaction; journal rows only exist alongside receipts, so
     * the identity check is then already proven.
     */
    @Suppress("LongParameterList")
    suspend fun create(room: RoomId, request: PostLockerChangesRequest, response: PostLockerChangesResponse, version: String,
        sourceOrder: Long, applied: Boolean = false, receiptAbsent: Boolean = false) = database.transaction(lock) {
        require(version.encodeToByteArray().size <= 128)
        check(receiptAbsent || find(room, request.writeRequestId) == null) { "Agent work identity reused" }
        admit(room)
        val id = key(room, request.writeRequestId)
        save(ServerAgentWork(key = id, roomId = room.rawValue, writeRequestId = request.writeRequestId,
            encodedRequest = request.toByteArray(), encodedOutcome = response.toByteArray(), agentVersion = version,
            effectKey = SHA256.digest("lockers.agent.effect.v1".encodeToByteArray() + id), sourceOrder = sourceOrder, createdAt = clock().coerceAtLeast(1),
            traceparent = producerTraceparent(),
            state = if (applied) AgentWorkState.APPLIED else AgentWorkState.PENDING, completedAt = if (applied) clock().coerceAtLeast(1) else 0))
    }
    /** Historical missing input is explicitly uncertain; completed delete receipts also get retention metadata. */
    suspend fun adopt(receipt: com.latenighthack.lockers.server.storage.v1.ServerWriteReceipt) = database.transaction(lock) {
        val room = RoomId(requireNotNull(receipt.roomId).rawValue)
        val old = find(room, receipt.requestId)
        if (old != null && old.encodedRequest.isNotEmpty()) return@transaction
        val response = PostLockerChangesResponse.fromByteArray(receipt.encodedOutcome)
        val state = when { response.agentPending || response.agentIndeterminate -> AgentWorkState.INDETERMINATE; response.agentFailed -> AgentWorkState.FAILED; else -> AgentWorkState.APPLIED }
        if (old == null) admit(room)
        val id = key(room, receipt.requestId)
        save(ServerAgentWork(key = id, roomId = room.rawValue, writeRequestId = receipt.requestId, encodedOutcome = receipt.encodedOutcome,
            state = state, effectKey = old?.effectKey ?: SHA256.digest("lockers.agent.effect.v1".encodeToByteArray() + id),
            createdAt = old?.createdAt ?: clock().coerceAtLeast(1), completedAt = if (AgentWorkState.terminal(state)) old?.completedAt?.takeIf { it > 0 } ?: clock().coerceAtLeast(1) else 0))
    }
    suspend fun snapshot(): QueueSnapshot {
        val schema = AgentWorkDefinitionV3
        return database.transaction(setOf(schema.storeName), TransactionMode.READ_ONLY) {
            val first = query(schema.storeName, schema.age.query(1, lower =
                OrderedKeyEncoding.long(1))).records.firstOrNull()?.let(::decode)
            QueueSnapshot(count(schema.storeName, schema.age.query(1)),
                count(schema.storeName, schema.age.query(1, lower = OrderedKeyEncoding.long(0), upper =
                    OrderedKeyEncoding.long(0))), first?.createdAt)
        }
    }
    suspend fun candidates(now: Long, after: LocalContinuation? = null): Pair<List<ServerAgentWork>, LocalContinuation?> {
        val page = database.query(AgentWorkDefinitionV3.storeName, AgentWorkDefinitionV3.due.query(64, upper =
            OrderedKeyEncoding.long(now), after = after))
        return page.records.map(::decode) to page.continuation
    }
    class Upcoming(val due: List<ServerAgentWork>, val nextDueAt: Long?, val continuation: LocalContinuation?)
    /** One indexed read: rows due by [now], or the earliest future due time within [horizon]. */
    suspend fun upcoming(now: Long, horizon: Long, after: LocalContinuation? = null): Upcoming {
        val page = database.query(AgentWorkDefinitionV3.storeName, AgentWorkDefinitionV3.due.query(64, upper =
            OrderedKeyEncoding.long(maxOf(now, horizon)), after = after))
        val rows = page.records.map(::decode)
        val due = rows.takeWhile { it.leaseUntil <= now }
        val next = rows.getOrNull(due.size)?.leaseUntil
        return Upcoming(due, next, if (next == null) page.continuation else null)
    }
    suspend fun claim(room: RoomId, id: ByteArray, expectedVersion: String, now: Long, leaseMs: Long = 30_000): ServerAgentWork? = database.transaction(lock) {
        val row = find(room, id) ?: return@transaction null
        if (AgentWorkState.terminal(row.state) || row.state == AgentWorkState.INDETERMINATE || row.leaseUntil > now) return@transaction null
        val first = firstActive(room)
        if (first?.key?.contentEquals(row.key) != true) return@transaction null
        claimRow(row, expectedVersion, now, leaseMs)
    }
    /** Whether the room has journaled agent work that has not settled; later work must queue behind it. */
    suspend fun hasActive(room: RoomId): Boolean = firstActive(room) != null
    private suspend fun firstActive(room: RoomId) = database.query(AgentWorkDefinitionV3.storeName,
        roomQuery(AgentWorkDefinitionV3.active, room.rawValue, 1)).records.firstOrNull()?.let(::decode)
    /** Claims the room's first active item when it is due; later items wait behind it in source order. */
    suspend fun claimNext(room: RoomId, expectedVersion: String, now: Long, leaseMs: Long = 30_000): ServerAgentWork? = database.transaction(lock) {
        val row = firstActive(room) ?: return@transaction null
        if (row.state == AgentWorkState.INDETERMINATE || row.leaseUntil > now) return@transaction null
        claimRow(row, expectedVersion, now, leaseMs)
    }
    private suspend fun claimRow(row: ServerAgentWork, expectedVersion: String, now: Long,
        leaseMs: Long): ServerAgentWork? {
        if ((row.state != AgentWorkState.READY && row.agentVersion != expectedVersion) || (row.state == AgentWorkState.RUNNING && row.agentVersion.isEmpty())) {
            save(row.copy(state = AgentWorkState.INDETERMINATE, leaseOwner = "", leaseUntil = 0, failure = "Execution interrupted or installed agent version changed"))
            return null
        }
        check(now <= Long.MAX_VALUE - leaseMs && leaseMs > 0)
        val claimed = row.copy(state = if (row.state == AgentWorkState.READY) row.state else AgentWorkState.RUNNING,
            leaseOwner = java.util.UUID.randomUUID().toString(), leaseUntil = now + leaseMs, attempts = if (row.attempts == Int.MAX_VALUE) row.attempts else row.attempts + 1)
        save(claimed); return claimed
    }
    private suspend fun current(claim: ServerAgentWork) = get(AgentWorkDefinitionV3.id.eq(claim.key))?.takeIf {
        it.leaseOwner == claim.leaseOwner && it.leaseOwner.isNotEmpty() && it.leaseUntil > clock() }
    suspend fun renew(claim: ServerAgentWork, now: Long, leaseMs: Long = 30_000): Boolean = database.transaction(lock) {
        val row = current(claim) ?: return@transaction false
        check(now <= Long.MAX_VALUE - leaseMs)
        save(row.copy(leaseUntil = now + leaseMs)); true
    }
    suspend fun result(claim: ServerAgentWork, writes: List<ServerAgentDerivedWrite>): ServerAgentWork? = database.transaction(lock) {
        val row = current(claim) ?: return@transaction null
        val ready = row.copy(state = AgentWorkState.READY, writes = writes)
        save(ready); ready
    }
    suspend fun <T> complete(claim: ServerAgentWork, state: Int, response: PostLockerChangesResponse, mutation: suspend () -> T): T? = database.transaction(lock) {
        val row = current(claim) ?: return@transaction null
        val value = mutation()
        save(row.copy(state = state, encodedOutcome = response.toByteArray(), leaseOwner = "", leaseUntil = 0,
            completedAt = if (AgentWorkState.terminal(state)) clock().coerceAtLeast(1) else 0))
        value
    }
    suspend fun retry(claim: ServerAgentWork, reason: String) = database.transaction(lock) {
        val row = current(claim) ?: return@transaction
        val delayMs = minOf(250L shl row.attempts.coerceIn(0, 6), 10_000)
        save(row.copy(state = AgentWorkState.PENDING, leaseOwner = "", leaseUntil = clock() + delayMs, failure = reason.take(1024)))
    }
    suspend fun interrupt(claim: ServerAgentWork, reason: String) = database.transaction(lock) {
        val row = current(claim) ?: return@transaction
        // A persisted result is recoverable without invoking an external effect again.
        save(row.copy(state = if (row.state == AgentWorkState.READY) AgentWorkState.READY else if (row.agentVersion.isEmpty()) AgentWorkState.INDETERMINATE else AgentWorkState.PENDING,
            leaseOwner = "", leaseUntil = 0, failure = reason.take(1024)))
    }
    suspend fun reconcile(room: RoomId, id: ByteArray, expectedEffectKey: ByteArray, writes: List<ServerAgentDerivedWrite>?, failed: Boolean): ServerAgentWork? = database.transaction(lock) {
        val row = find(room, id) ?: return@transaction null
        if (row.state != AgentWorkState.INDETERMINATE || !row.effectKey.contentEquals(expectedEffectKey)) return@transaction null
        val next = row.copy(state = if (failed) AgentWorkState.FAILED else AgentWorkState.READY, writes = writes.orEmpty(),
            leaseOwner = "", leaseUntil = 0, completedAt = if (failed) clock().coerceAtLeast(1) else 0)
        save(next); next
    }
    suspend fun prune(removeReceipt: suspend (RoomId, ByteArray) -> Unit) {
        // Global discovery never holds its journal lock while acquiring a room lock.
        val rows = database.query(AgentWorkDefinitionV3.storeName, AgentWorkDefinitionV3.completed.query(128,
            upper = OrderedKeyEncoding.long(clock() - retentionMs))).records.map(::decode)
        for (snapshot in rows) {
            val room = RoomId(snapshot.roomId)
            database.transaction(roomMutationKey(com.latenighthack.lockers.server.storage.v1.ServerRoomId(snapshot.roomId))) {
                database.transaction(lock) {
                    val row = find(room, snapshot.writeRequestId)
                    if (row != null && AgentWorkState.terminal(row.state) && row.completedAt == snapshot.completedAt) {
                        counters.adjust(capacity(room), -1)
                        removeReceipt(room, row.writeRequestId); delete(AgentWorkDefinitionV3.id.eq(row.key))
                    }
                }
            }
        }
    }
}
