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
    private val clock: () -> Long = System::currentTimeMillis,
    private val limits: com.latenighthack.lockers.server.ServerResourceLimits = com.latenighthack.lockers.server.ServerResourceLimits(),
    private val cpuAdmission: com.latenighthack.lockers.server.CpuAdmission = com.latenighthack.lockers.server.CpuAdmission(limits)) {
    private class ProofStore(database: Database) : Store<UsedSessionProof>(database, UsedSessionProofDefinitionV2) {
        suspend fun find(identity: ByteArray) = get(UsedSessionProofDefinitionV2.identity.eq(identity))
        suspend fun put(row: UsedSessionProof) = save(row)
    }
    private class OwnerStore(database: Database) : Store<UsedSessionProofOwner>(database, UsedSessionProofOwnersDefinitionV2) {
        suspend fun put(row: UsedSessionProofOwner) = save(row)
    }
    private val used = ProofStore(database)
    private val owners = OwnerStore(database)

    suspend fun <T> authorize(operation: String, sessionId: SessionId?, proof: SessionProof?,
        encodedRequest: ByteArray, rejected: () -> T, mutation: suspend () -> T): T {
        if (sessionId == null || sessionId.rawValue.size !in 1..128 || proof == null ||
            proof.nonce.size != 32 || proof.signature?.signingVersion != 2 || proof.toByteArray().size > 512 ||
            !com.latenighthack.lockers.server.CpuAdmission.signatureShape(proof.signature, required = true) || encodedRequest.size > 8 * 1024 * 1024) return rejected()
        val now = clock()
        if (proof.issuedAtMs < now - WINDOW_MS || proof.issuedAtMs > now + FUTURE_SKEW_MS) return rejected()
        cpuAdmission.require(encodedRequest.size, 3)
        val digest = SHA256.digest(encodedRequest)
        val identity = SHA256.digest(sessionId.toByteArray() + proof.nonce)
        return database.transaction("lockers.session-authority") {
            val storedId = ServerSessionId(sessionId.rawValue)
            val verificationKey = sessions.getSessionById(storedId)?.authorizedPublicKey
                ?: if (operation == SessionSigning.DESTROY) sessions.revokedVerificationKey(storedId) else null
            if (verificationKey == null || used.find(identity) != null) return@transaction rejected()
            if (!com.latenighthack.lockers.server.ProtocolValidation.publicKey(verificationKey)) return@transaction rejected()
            val valid = try {
                Secp256r1PublicKey.decode(verificationKey).verify(
                    SessionSigning.context(operation, sessionId, digest, proof.issuedAtMs, proof.nonce),
                    requireNotNull(proof.signature).signature,
                )
            } catch (cancelled: CancellationException) { throw cancelled }
            catch (_: Exception) { false }
            if (!valid) return@transaction rejected()
            enforceProofCapacity(sessionId, now)
            val expires = proof.issuedAtMs + WINDOW_MS + FUTURE_SKEW_MS
            used.put(UsedSessionProof(identity, expires))
            owners.put(UsedSessionProofOwner(identity, sessionId.rawValue, expires))
            mutation()
        }
    }

    private suspend fun enforceProofCapacity(sessionId: SessionId, now: Long) {
        val replay = UsedSessionProofDefinitionV2
        val ownership = UsedSessionProofOwnersDefinitionV2
        database.deleteBatch(replay.storeName, replay.expires.query(256, upper = now, upperInclusive = false))
        database.deleteBatch(ownership.storeName, ownership.expires.query(256, upper = now, upperInclusive = false))
        val total = database.count(replay.storeName, replay.identity.query(1))
        val owned = database.count(ownership.storeName, ownership.identity.query(1))
        val perSession = database.count(ownership.storeName, ownership.session.query(1,
            lower = sessionId.rawValue, upper = sessionId.rawValue))
        // Old rows lack recoverable owner IDs: conservatively charge all unowned rows to each SID.
        val unknown = (total - owned).coerceAtLeast(0)
        if (total >= limits.maxOutstandingProofs || perSession + unknown >= limits.maxOutstandingProofsPerSession) {
            throw com.latenighthack.ktbuf.net.RpcResponseException("", "RPC", com.latenighthack.ktbuf.proto.Codes.RESOURCE_EXHAUSTED,
                "Outstanding session proof capacity exhausted")
        }
    }

    companion object { const val WINDOW_MS = 60_000L; const val FUTURE_SKEW_MS = 5_000L }
}
