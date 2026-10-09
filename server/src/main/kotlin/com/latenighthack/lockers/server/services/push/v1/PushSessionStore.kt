package com.latenighthack.lockers.server.services.push.v1

import com.latenighthack.ktstore.*
import com.latenighthack.ktcrypto.SHA256
import com.latenighthack.ktcrypto.digest
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.server.storage.v2.*
import java.nio.ByteBuffer

interface PushSessionStore {
    suspend fun savePushInfo(pushInfo: ServerPushInfo)
    suspend fun getPushInfo(sessionId: ServerSessionId): ServerPushInfo?
    suspend fun deletePushInfo(sessionId: ServerSessionId)
    suspend fun applyCredential(sessionId: ServerSessionId, backend: Int, encoded: ByteArray?, revision: Long): Boolean =
        throw UnsupportedOperationException("PushSessionStore requires atomic credential revisions")
    suspend fun removeCredentialIfCurrent(sessionId: ServerSessionId, backend: Int, encoded: ByteArray): Boolean =
        throw UnsupportedOperationException("PushSessionStore requires credential compare-and-set")
}

class PushSessionStoreImpl(private val database: Database): PushSessionStore, Store<ServerPushInfo>(database, PushSessionStoreImplDefinitionV1) {
    private val sessionIdKey = PushSessionStoreImplDefinitionV1.sessionIdKey
    private class Credentials(database: Database) : Store<ServerPushCredential>(database, PushCredentialDefinitionV2) {
        suspend fun find(key: ByteArray) = get(PushCredentialDefinitionV2.key.eq(key))
        suspend fun put(row: ServerPushCredential) = save(row)
        suspend fun clear(sessionId: ByteArray) = delete(PushCredentialDefinitionV2.session.eq(sessionId))
    }
    private val credentials = Credentials(database)
    private fun key(sessionId: ServerSessionId, backend: Int) = pushSessionPrefix(sessionId.rawValue) + ByteBuffer.allocate(4).putInt(backend).array()

    override suspend fun savePushInfo(pushInfo: ServerPushInfo) = save(pushInfo)
    override suspend fun getPushInfo(sessionId: ServerSessionId): ServerPushInfo? = get(sessionIdKey.eq(sessionId.toByteArray()))
    override suspend fun deletePushInfo(sessionId: ServerSessionId) = database.transaction("lockers.session-authority") {
        delete(sessionIdKey.eq(sessionId.toByteArray()))
        // Canonical destruction reserves session IDs permanently before clearing this sidecar.
        credentials.clear(sessionId.rawValue)
    }

    override suspend fun applyCredential(sessionId: ServerSessionId, backend: Int, encoded: ByteArray?, revision: Long): Boolean =
        database.transaction("lockers.session-authority") {
            if (revision < 0 || backend !in 1..3) return@transaction false
            val key = key(sessionId, backend)
            val old = credentials.find(key)
            val digest = SHA256.digest(byteArrayOf(if (encoded == null) 0 else 1) + (encoded ?: byteArrayOf()))
            if (old != null && (revision < old.revision || (revision == 0L && old.revision > 0))) return@transaction false
            if (revision > 0 && old?.revision == revision) return@transaction !old.invalidated &&
                old.registered == (encoded != null) && old.payloadDigest.contentEquals(digest)
            val existing = getPushInfo(sessionId)?.registrations.orEmpty()
            val next = existing.filter { it.backend != backend } + if (encoded == null) emptyList() else
                listOf(ServerPushRegistration(backend = backend, encodedRegistration = encoded))
            if (next.isEmpty()) delete(sessionIdKey.eq(sessionId.toByteArray()))
            else save(ServerPushInfo(sessionId = sessionId, registrations = next))
            if (revision > 0) credentials.put(ServerPushCredential(key = key, sessionId = sessionId.rawValue,
                backend = backend, revision = revision, payloadDigest = digest, registered = encoded != null))
            true
        }

    override suspend fun removeCredentialIfCurrent(sessionId: ServerSessionId, backend: Int, encoded: ByteArray): Boolean =
        database.transaction("lockers.session-authority") {
            val info = getPushInfo(sessionId) ?: return@transaction false
            val current = info.registrations.firstOrNull { it.backend == backend } ?: return@transaction false
            if (!current.encodedRegistration.contentEquals(encoded)) return@transaction false
            val remaining = info.registrations.filter { it.backend != backend }
            if (remaining.isEmpty()) delete(sessionIdKey.eq(sessionId.toByteArray()))
            else save(info.copy(registrations = remaining))
            credentials.find(key(sessionId, backend))?.let { credentials.put(it.copy(invalidated = true)) }
            true
        }
}
