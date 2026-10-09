package com.latenighthack.lockers.server.services.session.v1

import com.latenighthack.ktstore.*
import java.nio.ByteBuffer

/** Additive ownership sidecar; the original replay identity/expiry codec stays frozen. */
data class UsedSessionProofOwner(val identity: ByteArray, val session: ByteArray, val expiresAt: Long)
object UsedSessionProofOwnersDefinitionV2 : StoreDefinition<UsedSessionProofOwner>(
    StoreName("used_session_proof_owners_v2"), "session-length-plus-identity-expiry-v2",
    { raw ->
        require(raw.isNotEmpty())
        val length = raw[0].toInt() and 255
        require(length in 1..128 && raw.size == length + 41)
        UsedSessionProofOwner(raw.copyOfRange(1 + length, 33 + length), raw.copyOfRange(1, 1 + length),
            ByteBuffer.wrap(raw, 33 + length, 8).long)
    },
    { row ->
        require(row.identity.size == 32 && row.session.size in 1..128)
        ByteBuffer.allocate(row.session.size + 41).put(row.session.size.toByte()).put(row.session)
            .put(row.identity).putLong(row.expiresAt).array()
    },
) {
    val identity = bytesIndex(IndexName("identity"), UsedSessionProofOwner::identity, "sha256-v2").also { primaryKey(it) }
    val session = bytesIndex(IndexName("session"), UsedSessionProofOwner::session, "opaque-id-v2")
    val expires = mappedIndex(IndexName("expiresAt"), UsedSessionProofOwner::expiresAt, object : StorageCodec<Long, ByteArray> {
        override fun encode(value: Long) = OrderedKeyEncoding.long(value)
        override fun key(name: IndexName) = StoreKey.SerializedKey(name.value)
    }, "ordered-long-v2")
}
