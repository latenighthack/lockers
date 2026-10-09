package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v1.*

interface SubscriptionStore {
    fun supportsIntentRevisions(): Boolean = false
    suspend fun <T> withIntent(sessionId: ServerSessionId, roomId: ServerRoomId, revision: Long, subscribed: Boolean, mutation: suspend () -> T): SubscriptionIntentResult<T> {
        if (revision != 0L) throw com.latenighthack.ktbuf.net.RpcResponseException("", "RPC", com.latenighthack.ktbuf.proto.Codes.UNIMPLEMENTED, "Custom subscription store does not support intent revisions")
        return SubscriptionIntentResult(0, false, mutation())
    }
    suspend fun <T> observeIntent(sessionId: ServerSessionId, roomId: ServerRoomId, revision: Long, mutation: suspend () -> T): SubscriptionIntentResult<T> {
        if (revision != 0L) throw com.latenighthack.ktbuf.net.RpcResponseException("", "RPC", com.latenighthack.ktbuf.proto.Codes.UNIMPLEMENTED, "Custom subscription store does not support intent revisions")
        return SubscriptionIntentResult(0, false, mutation())
    }

    suspend fun getAllSubscriptions(sessionId: ServerSessionId): List<ServerRoomId>

    suspend fun getAllSessions(roomId: ServerRoomId): List<ServerSessionId>

    suspend fun addSubscription(sessionId: ServerSessionId, roomId: ServerRoomId)

    suspend fun removeSubscription(sessionId: ServerSessionId, roomId: ServerRoomId)
}

class SubscriptionStoreImpl(private val database: Database, private val limits: com.latenighthack.lockers.server.ServerResourceLimits = com.latenighthack.lockers.server.ServerResourceLimits()) : SubscriptionStore, Store<ServerSubscription>(database, SubscriptionStoreImplDefinitionV1) {
    private val intents = SubscriptionIntents(database, limits)
    override fun supportsIntentRevisions() = true
    override suspend fun <T> withIntent(sessionId: ServerSessionId, roomId: ServerRoomId, revision: Long, subscribed: Boolean, mutation: suspend () -> T) = intents.apply(sessionId, roomId, revision, subscribed, mutation)
    override suspend fun <T> observeIntent(sessionId: ServerSessionId, roomId: ServerRoomId, revision: Long, mutation: suspend () -> T) = intents.observe(sessionId, roomId, revision, mutation)
    private val sessionIdKey = SubscriptionStoreImplDefinitionV1.sessionIdKey
    private val roomIdKey = SubscriptionStoreImplDefinitionV1.roomIdKey
    private val sessionIdAndRoomIdKey = SubscriptionStoreImplDefinitionV1.sessionIdAndRoomIdKey

    private suspend fun boundedRows(index: TypedIndex<ServerSubscription, ByteArray>, value: ByteArray, limit: Int): List<ServerSubscription> {
        val page = database.query(SubscriptionStoreImplDefinitionV1.storeName, index.query(limit + 1, lower = value, upper = value))
        if (page.records.size > limit) protocolCapacityExceeded("Historical subscription set exceeds finite capacity")
        return page.records.map { when (it) { is ServerSubscription -> it; is ByteArray -> SubscriptionStoreImplDefinitionV1.decode(it); else -> error("Invalid subscription row") } }
    }
    override suspend fun getAllSubscriptions(sessionId: ServerSessionId): List<ServerRoomId> =
        boundedRows(sessionIdKey, sessionId.toByteArray(), limits.maxSubscriptionsPerSession).mapNotNull { it.roomId }
    override suspend fun getAllSessions(roomId: ServerRoomId): List<ServerSessionId> =
        boundedRows(roomIdKey, roomId.toByteArray(), limits.maxSubscriptionsPerRoom).mapNotNull { it.sessionId }

    override suspend fun addSubscription(sessionId: ServerSessionId, roomId: ServerRoomId) = database.transaction(roomMutationKey(roomId)) {
        database.transaction("lockers.subscription-admission") {
            val relation = sessionIdAndRoomIdKey.eq(listOf(BoundStoreKey.SerializedKey(sessionIdKey.name.value, sessionId.toByteArray()),
                BoundStoreKey.SerializedKey(roomIdKey.name.value, roomId.toByteArray())))
            if (get(relation) == null) {
                val definition = SubscriptionStoreImplDefinitionV1
                val total = database.count(definition.storeName, sessionIdKey.query(1))
                val session = database.count(definition.storeName, sessionIdKey.query(1, lower = sessionId.toByteArray(), upper = sessionId.toByteArray()))
                val room = database.count(definition.storeName, roomIdKey.query(1, lower = roomId.toByteArray(), upper = roomId.toByteArray()))
                if (total >= limits.maxSubscriptions || session >= limits.maxSubscriptionsPerSession || room >= limits.maxSubscriptionsPerRoom)
                    resourceExhausted("Subscription capacity exhausted")
            }
            save(ServerSubscription(sessionId, roomId))
        }
    }

    override suspend fun removeSubscription(sessionId: ServerSessionId, roomId: ServerRoomId) = delete(sessionIdAndRoomIdKey.eq(
        listOf(
            BoundStoreKey.SerializedKey(sessionIdKey.name.value, sessionId.toByteArray()),
            BoundStoreKey.SerializedKey(roomIdKey.name.value, roomId.toByteArray())
        )
    ))
}
