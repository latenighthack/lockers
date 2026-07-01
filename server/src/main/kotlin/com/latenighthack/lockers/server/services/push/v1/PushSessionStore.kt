package com.latenighthack.lockers.server.services.push.v1

import com.latenighthack.ktstore.Store
import com.latenighthack.ktstore.StoreDelegate
import com.latenighthack.lockers.server.storage.v1.*

interface PushSessionStore {
    suspend fun savePushInfo(pushInfo: ServerPushInfo)

    suspend fun getPushInfo(sessionId: ServerSessionId): ServerPushInfo?

    suspend fun deletePushInfo(sessionId: ServerSessionId)
}

class PushSessionStoreImpl(delegate: StoreDelegate): PushSessionStore, Store<ServerPushInfo>(
    delegate,
    "push_session",
    ServerPushInfo::toByteArray,
    ServerPushInfo.Companion::fromByteArray
) {
    private val sessionIdKey = serializedIndex(ServerPushInfo::sessionId, ServerSessionId::toByteArray).also { primaryKey(it) }

    override suspend fun savePushInfo(pushInfo: ServerPushInfo) = save(pushInfo)

    override suspend fun getPushInfo(sessionId: ServerSessionId): ServerPushInfo? = get(sessionIdKey.eq(sessionId.toByteArray()))

    override suspend fun deletePushInfo(sessionId: ServerSessionId) = delete(sessionIdKey.eq(sessionId.toByteArray()))
}
