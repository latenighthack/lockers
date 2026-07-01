package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.ktstore.BoundStoreKey
import com.latenighthack.ktstore.Store
import com.latenighthack.ktstore.StoreDelegate
import com.latenighthack.lockers.server.storage.v1.*

interface SubscriptionStore {
    suspend fun getAllSubscriptions(sessionId: ServerSessionId): List<ServerRoomId>

    suspend fun getAllSessions(roomId: ServerRoomId): List<ServerSessionId>

    suspend fun addSubscription(sessionId: ServerSessionId, roomId: ServerRoomId)

    suspend fun removeSubscription(sessionId: ServerSessionId, roomId: ServerRoomId)
}

class SubscriptionStoreImpl(delegate: StoreDelegate) : SubscriptionStore, Store<ServerSubscription>(
    delegate,
    "subscriptions",
    ServerSubscription::toByteArray,
    ServerSubscription.Companion::fromByteArray
) {
    private val sessionIdKey = serializedIndex(
        ServerSubscription::sessionId,
        ServerSessionId::toByteArray
    )
    private val roomIdKey = serializedIndex(
        ServerSubscription::roomId,
        ServerRoomId::toByteArray
    )
    private val sessionIdAndRoomIdKey = compositeIndex(
        sessionIdKey,
        roomIdKey
    ).also { primaryKey(it) }

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
            BoundStoreKey.SerializedKey(sessionIdKey.name, sessionId.toByteArray()),
            BoundStoreKey.SerializedKey(roomIdKey.name, roomId.toByteArray())
        )
    ))
}
