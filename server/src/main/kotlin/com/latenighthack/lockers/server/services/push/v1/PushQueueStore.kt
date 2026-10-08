package com.latenighthack.lockers.server.services.push.v1

import com.latenighthack.ktstore.*
import com.latenighthack.ktcrypto.SHA256
import com.latenighthack.ktcrypto.digest
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.server.storage.v2.*

/** A lease token fences completion/retry; holding a stale snapshot never permits deleting a new claim. */
data class PushClaim(val push: ServerPush, val work: ServerPushWork)

interface PushQueueStore {
    val supportsClaims: Boolean get() = false
    suspend fun savePush(push: ServerPush)
    suspend fun getPendingPushes(): List<ServerPush>
    suspend fun clearPush(pushId: ServerPushId)
    suspend fun enqueue(push: ServerPush, stableIdentity: Boolean, maxPendingPerSession: Int = 1024): Boolean =
        throw UnsupportedOperationException("Durable push queue requires atomic enqueue and claims")
    suspend fun claim(backend: Int, owner: String, now: Long, limit: Int, leaseMs: Long = 30_000): List<PushClaim> =
        throw UnsupportedOperationException("Durable push queue requires atomic claims")
    suspend fun renew(claim: PushClaim, now: Long, leaseMs: Long = 30_000): Boolean =
        throw UnsupportedOperationException("Durable push queue requires leased completion")
    suspend fun finish(claim: PushClaim, parkedReason: String? = null, mutation: suspend () -> Unit = {}): Boolean =
        throw UnsupportedOperationException("Durable push queue requires leased completion")
    suspend fun retry(claim: PushClaim, nextAttempt: Int, retryAfter: Long): Boolean =
        throw UnsupportedOperationException("Durable push queue requires leased retries")
    suspend fun release(claim: PushClaim, retryAfter: Long): Boolean =
        throw UnsupportedOperationException("Durable push queue requires leased retries")
    suspend fun clearForSession(sessionId: ServerSessionId): Unit =
        throw UnsupportedOperationException("Durable push queue requires session cleanup")
}

class PushQueueStoreImpl(private val database: Database): PushQueueStore, Store<ServerPush>(database, PushQueueStoreImplDefinitionV1) {
    override val supportsClaims = true
    private val pushIdKey = PushQueueStoreImplDefinitionV1.pushIdKey
    private class WorkStore(database: Database) : Store<ServerPushWork>(database, PushWorkDefinitionV2) {
        suspend fun find(query: StoreRelation) = get(query)
        suspend fun put(row: ServerPushWork) = save(row)
        suspend fun remove(query: StoreRelation) = delete(query)
    }
    private val work = WorkStore(database)
    private var migrationAfter: LocalContinuation? = null
    private var migrationDone = false
    private val lock = "lockers.push-queue"

    private suspend fun metadata(push: ServerPush, stable: Boolean = false): ServerPushWork = ServerPushWork(
        pushId = requireNotNull(push.pushId).rawValue, sessionId = requireNotNull(push.sessionId).rawValue,
        backend = push.backend, payloadDigest = SHA256.digest(push.encodedPush), attempt = push.attempt,
        createdAt = System.currentTimeMillis(), deduplicated = stable,
    )
    private suspend fun find(id: ByteArray) = work.find(PushWorkDefinitionV2.id.eq(id))
    private fun active(current: ServerPushWork?, claim: PushClaim) = current != null && current.leaseOwner == claim.work.leaseOwner

    /** Admin retry explicitly resets a parked generation; ordinary gateway replay cannot do so. */
    override suspend fun savePush(push: ServerPush) = database.transaction(lock) {
        val old = find(requireNotNull(push.pushId).rawValue)
        save(push)
        work.put(metadata(push, old?.deduplicated == true))
    }

    override suspend fun enqueue(push: ServerPush, stableIdentity: Boolean, maxPendingPerSession: Int): Boolean = database.transaction(lock) {
        require(maxPendingPerSession > 0)
        val next = metadata(push, stableIdentity)
        val old = find(next.pushId)
        if (old != null) {
            require(old.payloadDigest.contentEquals(next.payloadDigest)) { "Push delivery identity reused for different content" }
            return@transaction false
        }
        val prefix = pushSessionPrefix(next.sessionId)
        val count = database.count(PushWorkDefinitionV2.storeName,
            PushWorkDefinitionV2.pendingSession.query(1, lower = prefix, upper = prefix + ByteArray(128) { -1 }))
        check(count < maxPendingPerSession) { "Push queue session capacity exceeded" }
        save(push)
        work.put(next)
        true
    }

    override suspend fun getPendingPushes(): List<ServerPush> = getAll()
    override suspend fun clearPush(pushId: ServerPushId) = database.transaction(lock) {
        delete(pushIdKey.eq(pushId.toByteArray()))
        work.remove(PushWorkDefinitionV2.id.eq(pushId.rawValue))
    }

    /** Backfill one bounded historical page. New enqueues always install their index atomically. */
    private suspend fun migratePage() {
        if (migrationDone) return
        val page = database.query(PushQueueStoreImplDefinitionV1.storeName, pushIdKey.query(128, after = migrationAfter))
        for (raw in page.records) {
            val push = if (raw is ServerPush) raw else ServerPush.fromByteArray(raw as ByteArray)
            val id = push.pushId ?: continue
            if (find(id.rawValue) == null && push.sessionId != null) work.put(metadata(push))
        }
        migrationAfter = page.continuation
        migrationDone = migrationAfter == null
    }

    override suspend fun claim(backend: Int, owner: String, now: Long, limit: Int, leaseMs: Long): List<PushClaim> = database.transaction(lock) {
        require(limit in 1..256 && leaseMs > 0)
        migratePage()
        val page = database.query(PushWorkDefinitionV2.storeName, PushWorkDefinitionV2.due.query(limit,
            lower = pushBackendTime(backend, 0), upper = pushBackendTime(backend, now)))
        val claimed = mutableListOf<PushClaim>()
        for (raw in page.records) {
            val row = if (raw is ServerPushWork) raw else ServerPushWork.fromByteArray(raw as ByteArray)
            val push = get(pushIdKey.eq(ServerPushId(row.pushId).toByteArray()))
            if (push == null) { work.remove(PushWorkDefinitionV2.id.eq(row.pushId)); continue }
            val leased = row.copy(leaseOwner = "$owner:${java.util.UUID.randomUUID()}", leaseUntil = now + leaseMs)
            work.put(leased)
            claimed.add(PushClaim(push, leased))
        }
        claimed
    }

    override suspend fun renew(claim: PushClaim, now: Long, leaseMs: Long): Boolean = database.transaction(lock) {
        val current = find(claim.work.pushId)
        if (!active(current, claim)) false else { work.put(current!!.copy(leaseUntil = now + leaseMs)); true }
    }
    override suspend fun finish(claim: PushClaim, parkedReason: String?, mutation: suspend () -> Unit): Boolean = database.transaction(lock) {
        val current = find(claim.work.pushId)
        if (!active(current, claim)) return@transaction false
        mutation()
        delete(pushIdKey.eq(ServerPushId(claim.work.pushId).toByteArray()))
        if (parkedReason != null) work.put(current!!.copy(leaseOwner = "", leaseUntil = 0, parkedReason = parkedReason, attempt = claim.push.attempt))
        else if (current!!.deduplicated) work.put(current.copy(leaseOwner = "", leaseUntil = 0, completedAt = System.currentTimeMillis().coerceAtLeast(1)))
        else work.remove(PushWorkDefinitionV2.id.eq(current.pushId))
        true
    }
    override suspend fun retry(claim: PushClaim, nextAttempt: Int, retryAfter: Long): Boolean = database.transaction(lock) {
        val current = find(claim.work.pushId)
        if (!active(current, claim)) return@transaction false
        save(claim.push.copy(attempt = nextAttempt))
        work.put(current!!.copy(leaseOwner = "", leaseUntil = 0, attempt = nextAttempt, retryAfter = retryAfter))
        true
    }
    override suspend fun release(claim: PushClaim, retryAfter: Long): Boolean = database.transaction(lock) {
        val current = find(claim.work.pushId)
        if (!active(current, claim)) false else { work.put(current!!.copy(leaseOwner = "", leaseUntil = 0, retryAfter = maxOf(current.retryAfter, retryAfter))); true }
    }
    override suspend fun clearForSession(sessionId: ServerSessionId) {
        // Historical rows lack the new session index; complete adoption before targeted erase.
        while (!migrationDone) database.transaction(lock) { migratePage() }
        val prefix = pushSessionPrefix(sessionId.rawValue)
        while (true) {
            val deleted = database.transaction(lock) {
                val page = database.query(PushWorkDefinitionV2.storeName, PushWorkDefinitionV2.session.query(128,
                    lower = prefix, upper = prefix + ByteArray(128) { -1 }))
                for (raw in page.records) {
                    val row = if (raw is ServerPushWork) raw else ServerPushWork.fromByteArray(raw as ByteArray)
                    delete(pushIdKey.eq(ServerPushId(row.pushId).toByteArray()))
                    work.remove(PushWorkDefinitionV2.id.eq(row.pushId))
                }
                page.records.size
            }
            if (deleted == 0) break
        }
    }
}
