package com.latenighthack.lockers.server.services.push.v1

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*

interface PushQueueStore {
    suspend fun savePush(push: ServerPush)

    suspend fun getPendingPushes(): List<ServerPush>

    suspend fun clearPush(pushId: ServerPushId)
}

class PushQueueStoreImpl(delegate: Database): PushQueueStore, Store<ServerPush>(delegate, PushQueueStoreImplDefinitionV1) {
    private val pushIdKey = PushQueueStoreImplDefinitionV1.pushIdKey

    override suspend fun savePush(push: ServerPush) = save(push)

    override suspend fun getPendingPushes(): List<ServerPush> = getAll()

    override suspend fun clearPush(pushId: ServerPushId) = delete(pushIdKey.eq(pushId.toByteArray()))
}
