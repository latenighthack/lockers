package com.latenighthack.lockers.server.services.session.v1

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*

private val ServerSession.revokedSessionIdEncoded: ByteArray get() = requireNotNull(sessionId).toByteArray()

/** Only the public verification key survives destruction, for fresh signed DESTROY confirmation.
 * This does not restore live authority or challenge material. Identity admission bounds its size. */
object RevokedSessionAuthorityDefinitionV2 : StoreDefinition<ServerSession>(
    StoreName("revoked_session_authorities_v2"), "ServerSession-destroy-authority-protobuf-v2",
    ServerSession.Companion::fromByteArray, ServerSession::toByteArray,
) {
    val sessionId = bytesIndex(IndexName("sessionId"), ServerSession::revokedSessionIdEncoded, "protobuf-v2")
        .also { primaryKey(it) }
}
