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
}
