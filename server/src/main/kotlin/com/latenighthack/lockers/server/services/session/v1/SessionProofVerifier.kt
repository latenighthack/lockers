package com.latenighthack.lockers.server.services.session.v1

import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.SessionSigning
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.storage.v1.ServerSessionId
import kotlinx.coroutines.CancellationException
import java.nio.ByteBuffer

data class UsedSessionProof(val identity: ByteArray, val expiresAt: Long)

/** Additive schema; expired proof identities may be pruned only after the replay window. */
object UsedSessionProofDefinitionV2 : StoreDefinition<UsedSessionProof>(
    StoreName("used_session_proofs_v2"), "sha256-plus-expiry-big-endian-v2",
    { raw -> require(raw.size == 40); UsedSessionProof(raw.copyOfRange(0, 32), ByteBuffer.wrap(raw, 32, 8).long) },
    { row -> require(row.identity.size == 32); ByteBuffer.allocate(40).put(row.identity).putLong(row.expiresAt).array() },
) {
    val identity = bytesIndex(IndexName("identity"), UsedSessionProof::identity, "sha256-v2").also { primaryKey(it) }
    val expires = mappedIndex(IndexName("expiresAt"), UsedSessionProof::expiresAt, object : StorageCodec<Long, ByteArray> {
        override fun encode(value: Long) = OrderedKeyEncoding.long(value)
        override fun key(name: IndexName) = StoreKey.SerializedKey(name.value)
    }, "ordered-long-v2")
}

/** Public unary RPC boundary. Proof consumption and local mutation are one transaction. */
class SessionProofVerifier(private val database: Database, private val sessions: SessionStore,
    private val clock: () -> Long = System::currentTimeMillis) {
    private class ProofStore(database: Database) : Store<UsedSessionProof>(database, UsedSessionProofDefinitionV2) {
        suspend fun find(identity: ByteArray) = get(UsedSessionProofDefinitionV2.identity.eq(identity))
        suspend fun put(row: UsedSessionProof) = save(row)
    }
    private val used = ProofStore(database)

    suspend fun <T> authorize(operation: String, sessionId: SessionId?, proof: SessionProof?,
        encodedRequest: ByteArray, rejected: () -> T, mutation: suspend () -> T): T {
        if (sessionId == null || sessionId.rawValue.size !in 1..128 || proof == null ||
            proof.nonce.size != 32 || proof.signature?.signingVersion != 2 || encodedRequest.size > 8 * 1024 * 1024) return rejected()
        val now = clock()
        if (proof.issuedAtMs < now - WINDOW_MS || proof.issuedAtMs > now + FUTURE_SKEW_MS) return rejected()
        val digest = SHA256.digest(encodedRequest)
        val identity = SHA256.digest(sessionId.toByteArray() + proof.nonce)
        database.transaction(setOf(UsedSessionProofDefinitionV2.storeName)) {
            deleteBatch(UsedSessionProofDefinitionV2.storeName, UsedSessionProofDefinitionV2.expires.query(256, upper = now, upperInclusive = false))
        }
        return database.transaction("lockers.session-authority") {
            val session = sessions.getSessionById(ServerSessionId(sessionId.rawValue)) ?: return@transaction rejected()
            if (used.find(identity) != null) return@transaction rejected()
            val valid = try {
                Secp256r1PublicKey.decode(session.authorizedPublicKey).verify(
                    SessionSigning.context(operation, sessionId, digest, proof.issuedAtMs, proof.nonce),
                    requireNotNull(proof.signature).signature,
                )
            } catch (cancelled: CancellationException) { throw cancelled }
            catch (_: Exception) { false }
            if (!valid) return@transaction rejected()
            used.put(UsedSessionProof(identity, proof.issuedAtMs + WINDOW_MS + FUTURE_SKEW_MS))
            mutation()
        }
    }

    companion object { const val WINDOW_MS = 60_000L; const val FUTURE_SKEW_MS = 5_000L }
}
