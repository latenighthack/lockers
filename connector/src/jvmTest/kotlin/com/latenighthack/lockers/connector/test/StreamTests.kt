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
import com.latenighthack.lockers.connector.storage.v1.StoredAck
import com.latenighthack.lockers.room.v1.PostLockerChangeRequest
import com.latenighthack.lockers.room.v1.SubscriptionRequest
import com.latenighthack.lockers.room.v1.fromByteArray
import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktbuf.net.RpcMethodSpecifier
import com.latenighthack.ktbuf.net.RpcResponse
import com.latenighthack.ktbuf.net.RpcServerStream
import com.latenighthack.ktbuf.proto.Codes
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

    /**
     * Delays stream connection setup, as a real transport does (DNS/TLS/upgrade). Surfaces the
     * request-flow race: anything the client emits right after its open request (pending acks)
     * had time to displace the open before the transport started sending.
     */
    private class SlowConnectRpcClient(private val delegate: com.latenighthack.ktbuf.net.RpcClient) :
        com.latenighthack.ktbuf.net.RpcClient {
        override suspend fun unaryCall(
            method: com.latenighthack.ktbuf.net.RpcMethodSpecifier,
            headers: Map<String, String>,
            request: ByteArray
        ) = delegate.unaryCall(method, headers, request)

        override suspend fun serverStreamingCall(
            method: com.latenighthack.ktbuf.net.RpcMethodSpecifier,
            block: suspend com.latenighthack.ktbuf.net.RpcServerStream.() -> Unit,
            readyCallback: () -> Unit
        ) {
            kotlinx.coroutines.delay(250)
            delegate.serverStreamingCall(method, block, readyCallback)
        }
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
    fun `pending stored acks do not block the session open`() =
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

            // Pending acks (e.g. the app died before the server confirmed them) are re-sent
            // immediately after the open request. That early second request must never displace
            // the open as the first frame on the wire — a client in this state used to lose its
            // open to the request-flow replay race and never truly connect.
            sessionStore.addAck(StoredAck(Random.nextBytes(32), Random.nextBytes(32)))
            sessionStore.addAck(StoredAck(Random.nextBytes(32), Random.nextBytes(32)))

            val version = Version(0, 0, 1)
            val stream = Stream(SlowConnectRpcClient(rpcClient), keySource, sessionStore, subscriptionStore, version)
            val incoming = CompletableDeferred<Event>()
            launch { incoming.complete(stream.events.first()) }
            stream.start()
            awaitConnected(stream)
            stream.subscribe(testRoomId, waitForSubscription = true)

            ShardedRoomServiceRpc(rpcClient).postLockerChange(PostLockerChangeRequest {
                roomId = testRoomId
                lockerId = testLockerId
                parentVersion = 0
                locker { open { encodedPayload = byteArrayOf() } }
                notification { payload { rawValue = byteArrayOf(4, 5, 6) } }
            })

            assertContentEquals(byteArrayOf(4, 5, 6), incoming.await().notification?.payload?.rawValue)
            stream.stop()
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

    @Test(timeout = 30_000)
    fun `a failing subscription reconcile retries in-session instead of parking until reconnect`() =
        runTestWithServer(Application::attachTestServices) { server, _ ->
            val storeDelegate = InMemoryStoreDelegate()
            val keyValueStore = KeyValueStore(InMemoryKeyValueStoreDelegate())
            val sessionStore = SessionStoreImpl(keyValueStore, storeDelegate)
            val subscriptionStore = SubscriptionStoreImpl(storeDelegate)
            val testRoomId = RoomId(Random.nextBytes(32))

            subscriptionStore.prepare()
            sessionStore.prepare()
            storeDelegate.createStores()

            // Fail the first two Subscription RPCs, then let it through. Previously the first
            // failure parked the room (left pending in the store) until the next reconnect, so
            // waitForSubscription hung; the reconcile must now retry in-session and settle.
            val faulty = FaultInjectingRpcClient(server.rpcClient) { method, attempt ->
                if (method.methodName == "Subscription" && attempt <= 2) {
                    throw FaultInjectingRpcClient.rpcError(Codes.UNAVAILABLE)
                }
            }

            val stream = Stream(faulty, FixedKeySource(Secp256r1KeyPair.generate()), sessionStore, subscriptionStore, Version(0, 0, 1))
            stream.start()
            awaitConnected(stream)

            // Returns only because the reconcile retried past the injected faults, in-session.
            stream.subscribe(testRoomId, waitForSubscription = true)

            stream.stop()
        }

    /** Faults the Subscription RPC only for [failRoom]; every other room passes through. */
    private class RoomFaultRpcClient(
        private val delegate: RpcClient,
        private val failRoom: RoomId,
    ) : RpcClient {
        override suspend fun unaryCall(
            method: RpcMethodSpecifier,
            headers: Map<String, String>,
            request: ByteArray,
        ): RpcResponse {
            if (method.methodName == "Subscription") {
                val decoded = SubscriptionRequest.fromByteArray(request)
                if (decoded.roomId?.rawValue?.contentEquals(failRoom.rawValue) == true) {
                    throw FaultInjectingRpcClient.rpcError(Codes.UNAVAILABLE)
                }
            }
            return delegate.unaryCall(method, headers, request)
        }

        override suspend fun serverStreamingCall(
            method: RpcMethodSpecifier,
            block: suspend RpcServerStream.() -> Unit,
            readyCallback: () -> Unit,
        ) = delegate.serverStreamingCall(method, block, readyCallback)
    }

    @Test(timeout = 30_000)
    fun `one room's failing subscription does not head-of-line-block another room`() =
        runTestWithServer(Application::attachTestServices) { server, _ ->
            val storeDelegate = InMemoryStoreDelegate()
            val keyValueStore = KeyValueStore(InMemoryKeyValueStoreDelegate())
            val sessionStore = SessionStoreImpl(keyValueStore, storeDelegate)
            val subscriptionStore = SubscriptionStoreImpl(storeDelegate)
            val badRoomId = RoomId(Random.nextBytes(32))
            val goodRoomId = RoomId(Random.nextBytes(32))

            subscriptionStore.prepare()
            sessionStore.prepare()
            storeDelegate.createStores()

            val stream = Stream(
                RoomFaultRpcClient(server.rpcClient, failRoom = badRoomId),
                FixedKeySource(Secp256r1KeyPair.generate()),
                sessionStore,
                subscriptionStore,
                Version(0, 0, 1),
            )
            stream.start()
            awaitConnected(stream)

            // The bad room's reconcile keeps failing and retries forever on its own coroutine.
            stream.subscribe(badRoomId, waitForSubscription = false)

            // A healthy room must still reconcile promptly. If the bad room's retry ran on the
            // shared reconcile loop it would head-of-line-block this one and the test would hang.
            stream.subscribe(goodRoomId, waitForSubscription = true)

            stream.stop()
        }
}
