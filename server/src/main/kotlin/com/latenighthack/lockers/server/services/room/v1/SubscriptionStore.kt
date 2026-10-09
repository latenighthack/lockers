package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*

interface SubscriptionStore {
    suspend fun getAllSubscriptions(sessionId: ServerSessionId): List<ServerRoomId>

    suspend fun getAllSessions(roomId: ServerRoomId): List<ServerSessionId>

    suspend fun addSubscription(sessionId: ServerSessionId, roomId: ServerRoomId)

    suspend fun removeSubscription(sessionId: ServerSessionId, roomId: ServerRoomId)
}

class SubscriptionStoreImpl(delegate: Database) : SubscriptionStore, Store<ServerSubscription>(delegate, SubscriptionStoreImplDefinitionV1) {
    private val sessionIdKey = SubscriptionStoreImplDefinitionV1.sessionIdKey
    private val roomIdKey = SubscriptionStoreImplDefinitionV1.roomIdKey
    private val sessionIdAndRoomIdKey = SubscriptionStoreImplDefinitionV1.sessionIdAndRoomIdKey

    override suspend fun getAllSubscriptions(sessionId: ServerSessionId): List<ServerRoomId> = getAll(sessionIdKey.eq(sessionId.toByteArray()))
        .mapNotNull {
            it.roomId
        }

    override suspend fun getAllSessions(roomId: ServerRoomId): List<ServerSessionId> = getAll(roomIdKey.eq(roomId.toByteArray()))
        .mapNotNull {
            it.sessionId
        }

    override suspend fun addSubscription(sessionId: ServerSessionId, roomId: ServerRoomId) = save(ServerSubscription(sessionId, roomId))

    override suspend fun removeSubscription(sessionId: ServerSessionId, roomId: ServerRoomId) = delete(sessionIdAndRoomIdKey.eq(
        listOf(
            BoundStoreKey.SerializedKey(sessionIdKey.name.value, sessionId.toByteArray()),
            BoundStoreKey.SerializedKey(roomIdKey.name.value, roomId.toByteArray())
        )
    ))
}
