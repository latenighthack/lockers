package com.latenighthack.lockers.connector.test

import kotlin.test.Test
import com.latenighthack.ktbuf.test.server.runTestWithServer
import com.latenighthack.ktcrypto.Secp256r1KeyPair
import com.latenighthack.ktcrypto.generate
import com.latenighthack.ktstore.InMemoryKeyValueStoreDelegate
import com.latenighthack.ktstore.InMemoryStoreDelegate
import com.latenighthack.ktstore.KeyValueStore
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.common.v1.Version
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.server.*
import com.latenighthack.lockers.connector.internal.ShardedRoomServiceRpc
import com.latenighthack.lockers.room.v1.PostLockerChangeRequest
import io.ktor.server.application.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.first
import kotlin.random.Random
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

class StreamTests {

    @Test
    fun `initialized → subscribed → event received`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val rpcClient = server.rpcClient
        val storeDelegate = InMemoryStoreDelegate()
        val keyValueStore = KeyValueStore(InMemoryKeyValueStoreDelegate())
        val sessionStore = SessionStoreImpl(keyValueStore, storeDelegate)
        val subscriptionStore = SubscriptionStoreImpl(storeDelegate)
        val sessionKeyPair = Secp256r1KeyPair.generate()
        val testRoomId = RoomId(Random.nextBytes(32))
        val testLockerId = LockerId(Random.nextBytes(32), LockerKeyspace { value = 1L })
        val incomingEvent = CompletableDeferred<Event>()

        val keySource = object : AuthenticationKeySource {
            override suspend fun getSessionKeyPair(): Secp256r1KeyPair {
                return sessionKeyPair
            }

            override suspend fun hasSessionKeyPair(): Boolean = true

            override suspend fun generateSessionKeyPair() {
            }

            override suspend fun revokeKeys() {
            }
        }

        subscriptionStore.prepare()
        sessionStore.prepare()
        storeDelegate.createStores()

        val stream = Stream(
            rpcClient,
            keySource,
            sessionStore,
            subscriptionStore,
            Version(0, 0, 1)
        )

        val isConnected = CompletableDeferred<Boolean>()

        launch {
            stream.isConnected
                .filter { it }
                .first()

            isConnected.complete(true)
        }

        launch {
            incomingEvent.complete(stream.events.first())
        }

        stream.start()

        isConnected.await()

        stream.subscribe(testRoomId, waitForSubscription = true)

        val roomRpc = ShardedRoomServiceRpc(rpcClient)

        val postResponse = roomRpc.postLockerChange(PostLockerChangeRequest {
            roomId = testRoomId
            lockerId = testLockerId
            parentVersion = 0
            locker {
                open {
                    encodedPayload = byteArrayOf()
                }
            }
            notification {
                push {
                    title = "This is..."
                    body = "...a test"
                }
                payload {
                    rawValue = byteArrayOf(1, 2, 3)
                }
            }
        })

        val event = incomingEvent.await()

        assertContentEquals(byteArrayOf(1, 2, 3), event.notification?.payload?.rawValue)
    }

    private class FixedKeySource(private val keyPair: Secp256r1KeyPair) : AuthenticationKeySource {
        override suspend fun getSessionKeyPair(): Secp256r1KeyPair = keyPair
        override suspend fun hasSessionKeyPair(): Boolean = true
        override suspend fun generateSessionKeyPair() {}
        override suspend fun revokeKeys() {}
    }

    private suspend fun CoroutineScope.awaitConnected(stream: Stream): SessionId {
        stream.isConnected.filter { it }.first()
        return stream.sessionId.filterNotNull().first()
    }

    @Test
    fun `desynced sequence bytes recover the same session and its queued events`() =
        runTestWithServer(Application::attachTestServices) { server, _ ->
            val rpcClient = server.rpcClient
            val storeDelegate = InMemoryStoreDelegate()
            val keyValueStore = KeyValueStore(InMemoryKeyValueStoreDelegate())
            val sessionStore = SessionStoreImpl(keyValueStore, storeDelegate)
            val subscriptionStore = SubscriptionStoreImpl(storeDelegate)
            val keySource = FixedKeySource(Secp256r1KeyPair.generate())
            val testRoomId = RoomId(Random.nextBytes(32))
            val testLockerId = LockerId(Random.nextBytes(32), LockerKeyspace { value = 1L })

            subscriptionStore.prepare()
            sessionStore.prepare()
            storeDelegate.createStores()

            val version = Version(0, 0, 1)
            val stream1 = Stream(rpcClient, keySource, sessionStore, subscriptionStore, version)
            stream1.start()
            val firstSessionId = awaitConnected(stream1)
            stream1.subscribe(testRoomId, waitForSubscription = true)
            stream1.stop()

            // Event posted while the client is offline: queues in the session inbox.
            ShardedRoomServiceRpc(rpcClient).postLockerChange(PostLockerChangeRequest {
                roomId = testRoomId
                lockerId = testLockerId
                parentVersion = 0
                locker { open { encodedPayload = byteArrayOf() } }
                notification { payload { rawValue = byteArrayOf(7, 8, 9) } }
            })

            // Desync: the client's stored sequence bytes no longer match the server's material
            // (as when the app dies between the server's rotation and the client's persist).
            sessionStore.updateNextSequenceBytes(Random.nextBytes(32))

            val stream2 = Stream(rpcClient, keySource, sessionStore, subscriptionStore, version)
            val queued = CompletableDeferred<Event>()
            launch { queued.complete(stream2.events.first()) }
            stream2.start()
            val recoveredSessionId = awaitConnected(stream2)

            // Same session (recovered via the INVALID_SEQUENCE material, not re-created), and
            // the offline backlog is delivered with the open.
            assertContentEquals(firstSessionId.rawValue, recoveredSessionId.rawValue)
            assertContentEquals(byteArrayOf(7, 8, 9), queued.await().notification?.payload?.rawValue)
            stream2.stop()
        }

    @Test
    fun `unrecoverable signature mismatch falls back to a fresh session`() =
        runTestWithServer(Application::attachTestServices) { server, _ ->
            val rpcClient = server.rpcClient
            val storeDelegate = InMemoryStoreDelegate()
            val keyValueStore = KeyValueStore(InMemoryKeyValueStoreDelegate())
            val sessionStore = SessionStoreImpl(keyValueStore, storeDelegate)
            val subscriptionStore = SubscriptionStoreImpl(storeDelegate)

            subscriptionStore.prepare()
            sessionStore.prepare()
            storeDelegate.createStores()

            val version = Version(0, 0, 1)
            val stream1 = Stream(rpcClient, FixedKeySource(Secp256r1KeyPair.generate()), sessionStore, subscriptionStore, version)
            stream1.start()
            val firstSessionId = awaitConnected(stream1)
            stream1.stop()

            // A different session key: every open signature fails no matter the material, so the
            // client must give up on the session after INVALID_SEQUENCE_WIPE_THRESHOLD attempts
            // and create a fresh one instead of locking out forever.
            val stream2 = Stream(rpcClient, FixedKeySource(Secp256r1KeyPair.generate()), sessionStore, subscriptionStore, version)
            stream2.start()
            val freshSessionId = awaitConnected(stream2)

            assertEquals(false, firstSessionId.rawValue.contentEquals(freshSessionId.rawValue))
            stream2.stop()
        }
}
