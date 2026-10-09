package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.lockers.connector.test.runOwnedTestWithServer as runTestWithServer
import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.server.*
import com.latenighthack.lockers.connector.test.ownedRpcClient as rpcClient
import com.latenighthack.lockers.server.storage.v1.ServerSessionId
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.*

class ReviewNotificationContextTests {
    @Test(timeout = 20_000)
    fun `context authenticated partial push metadata survives live and offline replay`() = runTestWithServer({
        capturedCore = null
        attachTestServicesWith { capturedCore = it }
    }) { server, _ -> withContext(Dispatchers.Default) {
        val encodedContexts = CopyOnWriteArrayList<NotificationContext>()
        val decodedContexts = CopyOnWriteArrayList<NotificationContext>()
        val codec = object : NotificationCodec {
            suspend fun binding(context: NotificationContext) = SHA256.digest(
                context.roomId.rawValue + context.lockerId.rawValue +
                    "${context.keyspace.value}:${context.title}:${context.body}".encodeToByteArray())
            override suspend fun encode(context: NotificationContext, payload: ByteArray): ByteArray {
                encodedContexts += context; return binding(context) + payload
            }
            override suspend fun decode(context: NotificationContext, payload: ByteArray): ByteArray {
                assertContentEquals(binding(context), payload.copyOfRange(0, 32), "transport metadata changed the authenticated context")
                decodedContexts += context; return payload.copyOfRange(32, payload.size)
            }
        }
        val codecs = NotificationCodecs.of(codec)
        val receiveDatabase = ConnectorStorage.inMemory()
        val receiveValues = KeyValueStore(InMemoryKeyValueStoreDelegate())
        val receiveKey = Secp256r1KeyPair.generate()
        suspend fun client(rpc: RpcClient, db: Database = ConnectorStorage.inMemory(), values: KeyValueStore = KeyValueStore(InMemoryKeyValueStoreDelegate()), key: Secp256r1KeyPair? = null): LockersClient {
            val actualKey = key ?: Secp256r1KeyPair.generate()
            val client = createOwnedTestClient(rpc, db, values, object : AuthenticationKeySource {
                override suspend fun getSessionKeyPair() = actualKey
                override suspend fun hasSessionKeyPair() = true
                override suspend fun generateSessionKeyPair() {}
                override suspend fun revokeKeys() {}
            }, Version(), codecs = codecs, coroutineContext = currentCoroutineContext())
            try { client.awaitConnected(); return client } catch (failure: Throwable) { client.closeAndJoin(); throw failure }
        }
        var receiver: LockersClient? = null
        var sender: LockersClient? = null
        try {
            val room = RoomId(byteArrayOf(1)); val id = LockerId(byteArrayOf(2), LockerKeyspace(7))
            val receiving = client(server.rpcClient, receiveDatabase, receiveValues, receiveKey).also { receiver = it }
            val sending = client(server.rpcClient).also { sender = it }
            receiving.lockers.subscribeToRoom(room)
            sending.lockers.updateLocker(room, id, { push { body = "body only" }; payload { rawValue = "live".encodeToByteArray() } }) { byteArrayOf(1) }
            val live = withTimeout(5_000) { receiving.notificationsAfter(0).first() }
            assertEquals("live", live.notification.payload.decodeToString())
            val session = requireNotNull(receiving.sessionId.value)
            receiving.closeAndJoin(); receiver = null
            sending.lockers.updateLocker(room, id, { push { title = "title only" }; payload { rawValue = "offline".encodeToByteArray() } }) { byteArrayOf(2) }
            withTimeout(5_000) {
                while (requireNotNull(capturedCore).sessionInboxStore.getAllEvents(ServerSessionId(session.rawValue)).isEmpty()) delay(10)
            }
            val replacement = client(server.rpcClient, receiveDatabase, receiveValues, receiveKey).also { receiver = it }
            assertContentEquals(session.rawValue, replacement.sessionId.value!!.rawValue)
            val replay = withTimeout(5_000) { replacement.notificationsAfter(live.cursor).first() }
            assertEquals("offline", replay.notification.payload.decodeToString())
            assertEquals(listOf("" to "body only", "title only" to ""), encodedContexts.map { it.title to it.body })
            assertEquals(encodedContexts.map { it.title to it.body }, decodedContexts.map { it.title to it.body })
        } finally { receiver?.closeAndJoin(); sender?.closeAndJoin(); receiveDatabase.close() }
    } }

    private var capturedCore: ServerCore? = null
}
