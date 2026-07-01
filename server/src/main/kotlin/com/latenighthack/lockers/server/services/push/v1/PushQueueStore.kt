package com.latenighthack.lockers.server.services.push.v1

import com.latenighthack.ktstore.Store
import com.latenighthack.ktstore.StoreDelegate
import com.latenighthack.lockers.server.storage.v1.*

interface PushQueueStore {
    suspend fun savePush(push: ServerPush)

    suspend fun getPendingPushes(): List<ServerPush>

    suspend fun clearPush(pushId: ServerPushId)
}

class PushQueueStoreImpl(delegate: StoreDelegate): PushQueueStore, Store<ServerPush>(
    delegate,
    "push",
    ServerPush::toByteArray,
    ServerPush.Companion::fromByteArray
) {
    private val pushIdKey = serializedIndex(ServerPush::pushId, ServerPushId::toByteArray).also { primaryKey(it) }

    override suspend fun savePush(push: ServerPush) = save(push)

    override suspend fun getPendingPushes(): List<ServerPush> = getAll()

    override suspend fun clearPush(pushId: ServerPushId) = delete(pushIdKey.eq(pushId.toByteArray()))
}
