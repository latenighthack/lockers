package com.latenighthack.lockers.server.services.push.v1

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.server.storage.v2.*

interface PushDeadLetterStore {
    suspend fun saveDeadLetter(deadLetter: ServerDeadLetter)
    suspend fun getAllDeadLetters(): List<ServerDeadLetter>
    suspend fun getDeadLetter(pushId: ServerPushId): ServerDeadLetter?
    suspend fun deleteDeadLetter(pushId: ServerPushId)
    suspend fun counts(): Map<Int, Long> = throw UnsupportedOperationException("Indexed dead-letter counts required")
    suspend fun page(limit: Int = 128, after: LocalContinuation? = null): Pair<List<ServerDeadLetter>, LocalContinuation?> = throw UnsupportedOperationException("Indexed dead-letter pages required")
    suspend fun clearForSession(sessionId: ServerSessionId): Unit = throw UnsupportedOperationException("Indexed dead-letter cleanup required")
}

class PushDeadLetterStoreImpl(private val database: Database) : PushDeadLetterStore, Store<ServerDeadLetter>(database, PushDeadLetterStoreImplDefinitionV1) {
    private val pushIdKey = PushDeadLetterStoreImplDefinitionV1.pushIdKey
    private class IndexStore(db: Database): Store<ServerPushWork>(db, PushDeadLetterIndexDefinitionV2) {
        suspend fun put(row: ServerPushWork) = save(row)
        suspend fun remove(id: ByteArray) = delete(PushDeadLetterIndexDefinitionV2.id.eq(id))
    }
    private val index = IndexStore(database)
    @Volatile private var migrationDone = false
    private var migrationAfter: LocalContinuation? = null
    private val lock = "lockers.push-queue"
    private fun ServerDeadLetter.metadata() = ServerPushWork(pushId = requireNotNull(pushId).rawValue,
        sessionId = sessionId?.rawValue ?: ByteArray(0), backend = backend, createdAt = deadLetteredAt)
    override suspend fun saveDeadLetter(deadLetter: ServerDeadLetter) = database.transaction(lock) {
        if (getDeadLetter(requireNotNull(deadLetter.pushId)) == null) check(database.count(PushDeadLetterStoreImplDefinitionV1.storeName, pushIdKey.query(1)) < 1_000_000) { "Dead-letter retained capacity exceeded" }
        save(deadLetter); index.put(deadLetter.metadata())
    }
    override suspend fun getAllDeadLetters(): List<ServerDeadLetter> = getAll() // compatibility only; administration uses bounded pages
    override suspend fun getDeadLetter(pushId: ServerPushId): ServerDeadLetter? = get(pushIdKey.eq(pushId.toByteArray()))
    override suspend fun deleteDeadLetter(pushId: ServerPushId) = database.transaction(lock) {
        delete(pushIdKey.eq(pushId.toByteArray())); index.remove(pushId.rawValue)
    }
    private suspend fun initialize() {
        while (!migrationDone) database.transaction(lock) {
            if (migrationDone) return@transaction
            val page = database.query(PushDeadLetterStoreImplDefinitionV1.storeName, pushIdKey.query(128, after = migrationAfter))
            for (raw in page.records) {
                val row = if (raw is ServerDeadLetter) raw else ServerDeadLetter.fromByteArray(raw as ByteArray)
                // A concurrent operator may already have purged/retried this snapshot.
                getDeadLetter(requireNotNull(row.pushId))?.let { index.put(it.metadata()) }
            }
            migrationAfter = page.continuation; migrationDone = migrationAfter == null
        }
    }
    override suspend fun counts(): Map<Int, Long> {
        initialize()
        return database.transaction(lock) { (0..3).associateWith { backend ->
            val key = OrderedKeyEncoding.int(backend)
            database.count(PushDeadLetterIndexDefinitionV2.storeName, PushDeadLetterIndexDefinitionV2.backend.query(1, lower = key, upper = key))
        } }
    }
    override suspend fun page(limit: Int, after: LocalContinuation?): Pair<List<ServerDeadLetter>, LocalContinuation?> {
        require(limit in 1..256)
        val rows = database.query(PushDeadLetterStoreImplDefinitionV1.storeName, pushIdKey.query(limit, after = after))
        return rows.records.map { if (it is ServerDeadLetter) it else ServerDeadLetter.fromByteArray(it as ByteArray) } to rows.continuation
    }
    override suspend fun clearForSession(sessionId: ServerSessionId) {
        initialize()
        val prefix = pushSessionPrefix(sessionId.rawValue)
        while (true) {
            val removed = database.transaction(lock) {
                val rows = database.query(PushDeadLetterIndexDefinitionV2.storeName, PushDeadLetterIndexDefinitionV2.session.query(128, lower = prefix, upper = prefix + ByteArray(128) { -1 }))
                rows.records.forEach { raw ->
                    val row = if (raw is ServerPushWork) raw else ServerPushWork.fromByteArray(raw as ByteArray)
                    deleteDeadLetter(ServerPushId(row.pushId))
                }
                rows.records.size
            }
            if (removed == 0) return
        }
    }
}
