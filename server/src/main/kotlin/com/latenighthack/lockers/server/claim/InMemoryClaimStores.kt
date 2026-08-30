package com.latenighthack.lockers.server.claim

import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.common.v1.SessionId
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * In-memory [RoomClaimStore] with the exact conditional semantics of [JdbcRoomClaimStore] (steal
 * only if expired; epoch bumps only on takeover; release guarded by owner). Lives in the main
 * source set — like [com.latenighthack.lockers.server.services.room.v1.LocalRoomOwnership] — so
 * both the server tests and embedders can wire claim mode without a database. The injectable
 * [clock] stands in for the DB's `now()` as the single clock authority.
 */
class InMemoryRoomClaimStore(
    private val clock: () -> Long = System::currentTimeMillis,
) : RoomClaimStore {
    private data class Entry(val nodeId: String, val nodeAddr: String, val epoch: Long, val expiresAt: Long)

    private val mutex = Mutex()
    private val rows = HashMap<RoomId, Entry>()

    override suspend fun prepare() {}

    override suspend fun claim(roomId: RoomId, nodeId: String, nodeAddr: String, ttlMs: Long): RoomClaimRow =
        mutex.withLock {
            val now = clock()
            val existing = rows[roomId]
            val next = when {
                existing == null -> Entry(nodeId, nodeAddr, epoch = 1, expiresAt = now + ttlMs)
                existing.nodeId == nodeId -> existing.copy(nodeAddr = nodeAddr, expiresAt = now + ttlMs)
                existing.expiresAt < now ->
                    Entry(nodeId, nodeAddr, epoch = existing.epoch + 1, expiresAt = now + ttlMs)
                else -> existing // valid foreign owner: no steal, return their row
            }
            rows[roomId] = next
            RoomClaimRow(next.nodeId, next.nodeAddr, next.epoch)
        }

    override suspend fun renewAll(nodeId: String, ttlMs: Long): Set<RoomId> = mutex.withLock {
        val now = clock()
        buildSet {
            for ((roomId, entry) in rows) {
                if (entry.nodeId == nodeId && entry.expiresAt >= now) {
                    rows[roomId] = entry.copy(expiresAt = now + ttlMs)
                    add(roomId)
                }
            }
        }
    }

    override suspend fun release(roomId: RoomId, nodeId: String) {
        mutex.withLock {
            if (rows[roomId]?.nodeId == nodeId) rows.remove(roomId)
        }
    }

    override suspend fun releaseAll(nodeId: String) {
        mutex.withLock { rows.entries.removeIf { it.value.nodeId == nodeId } }
    }

    override suspend fun lookup(roomId: RoomId): RoomClaimRow? = mutex.withLock {
        rows[roomId]?.let { RoomClaimRow(it.nodeId, it.nodeAddr, it.epoch) }
    }

    override suspend fun ping() {}
}

/** In-memory [SessionGatewayStore]; expired rows are invisible to [lookup], per the interface. */
class InMemorySessionGatewayStore(
    private val clock: () -> Long = System::currentTimeMillis,
) : SessionGatewayStore {
    private data class Entry(val nodeId: String, val nodeAddr: String, val expiresAt: Long)

    private val mutex = Mutex()
    private val rows = HashMap<SessionId, Entry>()

    override suspend fun prepare() {}

    override suspend fun upsert(sessionId: SessionId, nodeId: String, nodeAddr: String, ttlMs: Long) {
        mutex.withLock { rows[sessionId] = Entry(nodeId, nodeAddr, clock() + ttlMs) }
    }

    override suspend fun renewAll(nodeId: String, ttlMs: Long): Set<SessionId> = mutex.withLock {
        val now = clock()
        buildSet {
            for ((sessionId, entry) in rows) {
                if (entry.nodeId == nodeId && entry.expiresAt >= now) {
                    rows[sessionId] = entry.copy(expiresAt = now + ttlMs)
                    add(sessionId)
                }
            }
        }
    }

    override suspend fun delete(sessionId: SessionId, nodeId: String) {
        mutex.withLock {
            if (rows[sessionId]?.nodeId == nodeId) rows.remove(sessionId)
        }
    }

    override suspend fun releaseAll(nodeId: String) {
        mutex.withLock { rows.entries.removeIf { it.value.nodeId == nodeId } }
    }

    override suspend fun lookup(sessionId: SessionId): SessionGatewayRow? = mutex.withLock {
        rows[sessionId]?.takeIf { it.expiresAt >= clock() }?.let { SessionGatewayRow(it.nodeId, it.nodeAddr) }
    }
}
