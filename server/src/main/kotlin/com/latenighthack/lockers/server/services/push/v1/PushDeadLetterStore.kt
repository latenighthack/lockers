package com.latenighthack.lockers.server.services.push.v1

import com.latenighthack.ktstore.Store
import com.latenighthack.ktstore.StoreDelegate
import com.latenighthack.lockers.server.storage.v1.*

interface PushDeadLetterStore {
    suspend fun saveDeadLetter(deadLetter: ServerDeadLetter)

    suspend fun getAllDeadLetters(): List<ServerDeadLetter>

    suspend fun getDeadLetter(pushId: ServerPushId): ServerDeadLetter?

    suspend fun deleteDeadLetter(pushId: ServerPushId)
}

class PushDeadLetterStoreImpl(delegate: StoreDelegate) : PushDeadLetterStore, Store<ServerDeadLetter>(
    delegate,
    "push_deadletter",
    ServerDeadLetter::toByteArray,
    ServerDeadLetter.Companion::fromByteArray
) {
    private val pushIdKey = serializedIndex(ServerDeadLetter::pushId, ServerPushId::toByteArray).also { primaryKey(it) }

    override suspend fun saveDeadLetter(deadLetter: ServerDeadLetter) = save(deadLetter)

    override suspend fun getAllDeadLetters(): List<ServerDeadLetter> = getAll()

    override suspend fun getDeadLetter(pushId: ServerPushId): ServerDeadLetter? = get(pushIdKey.eq(pushId.toByteArray()))

    override suspend fun deleteDeadLetter(pushId: ServerPushId) = delete(pushIdKey.eq(pushId.toByteArray()))
}
