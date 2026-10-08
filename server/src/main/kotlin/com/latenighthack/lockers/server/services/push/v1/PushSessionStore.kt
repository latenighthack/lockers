package com.latenighthack.lockers.server.services.push.v1

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*

interface PushSessionStore {
    suspend fun savePushInfo(pushInfo: ServerPushInfo)

    suspend fun getPushInfo(sessionId: ServerSessionId): ServerPushInfo?

    suspend fun deletePushInfo(sessionId: ServerSessionId)
}

class PushSessionStoreImpl(delegate: Database): PushSessionStore, Store<ServerPushInfo>(delegate, PushSessionStoreImplDefinitionV1) {
    private val sessionIdKey = PushSessionStoreImplDefinitionV1.sessionIdKey

    override suspend fun savePushInfo(pushInfo: ServerPushInfo) = save(pushInfo)

    override suspend fun getPushInfo(sessionId: ServerSessionId): ServerPushInfo? = get(sessionIdKey.eq(sessionId.toByteArray()))

    override suspend fun deletePushInfo(sessionId: ServerSessionId) = delete(sessionIdKey.eq(sessionId.toByteArray()))
}
