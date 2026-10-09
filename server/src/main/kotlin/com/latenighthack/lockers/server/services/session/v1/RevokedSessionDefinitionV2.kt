package com.latenighthack.lockers.server.services.session.v1

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*

private val ServerSessionId.serializedIdentity: ByteArray get() = toByteArray()

/** Permanent identity reservation: queued pre-revocation events cannot reach a reused ID. */
object RevokedSessionDefinitionV2 : StoreDefinition<ServerSessionId>(
    StoreName("revoked_sessions_v2"), "ServerSessionId-protobuf-v2",
    ServerSessionId.Companion::fromByteArray, ServerSessionId::toByteArray,
) {
    val sessionId = bytesIndex(IndexName("sessionId"), ServerSessionId::serializedIdentity, "protobuf-v2").also { primaryKey(it) }
}
