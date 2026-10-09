package com.latenighthack.lockers.server.services.push.v1

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v2.*

object PushCredentialDefinitionV2 : StoreDefinition<ServerPushCredential>(
    StoreName("push_credential_v2"), "ServerPushCredential-protobuf-v2",
    ServerPushCredential.Companion::fromByteArray, ServerPushCredential::toByteArray,
) {
    val key = bytesIndex(IndexName("session_backend"), ServerPushCredential::key, "length-prefix-session-backend-v2").also { primaryKey(it) }
    val session = bytesIndex(IndexName("session_raw"), ServerPushCredential::sessionId, "raw-bytes-v2")
}
