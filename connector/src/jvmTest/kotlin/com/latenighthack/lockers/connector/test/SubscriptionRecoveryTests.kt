package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktbuf.test.server.runTestWithServer
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.*
import io.ktor.server.application.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlin.test.*

class SubscriptionRecoveryTests {
    private class GatedClient(private val delegate: RpcClient) : RpcClient {
        data class Call(val request: SubscriptionRequest, val release: CompletableDeferred<Unit>)
        val calls = Channel<Call>(Channel.UNLIMITED)
        override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray): RpcResponse {
            if (method.methodName == "Subscription") {
                val release = CompletableDeferred<Unit>()
                calls.send(Call(SubscriptionRequest.fromByteArray(request), release))
                release.await()
            }
            return delegate.unaryCall(method, headers, request)
        }
        override suspend fun serverStreamingCall(method: RpcMethodSpecifier, block: suspend RpcServerStream.() -> Unit, readyCallback: () -> Unit) =
            delegate.serverStreamingCall(method, block, readyCallback)
    }

    @Test(timeout = 15_000)
    fun `session replacement requires a new acknowledgment and restores every room`() =
        runTestWithServer(Application::attachTestServices) { server, _ -> withContext(Dispatchers.Default) {
            val store = InMemoryStoreDelegate()
            val sessions = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), store)
            val subscriptions = SubscriptionStoreImpl(store)
            sessions.prepare(); subscriptions.prepare(); store.createStores()
            val source = MutableStateFlow<SessionId?>(SessionId(byteArrayOf(1)))
            val rpc = GatedClient(server.rpcClient)
            val controller = SubscriptionController(rpc, subscriptions, sessions, source)
            val room = RoomId(byteArrayOf(2))
            controller.startWatchingSubscriptions()
            try {
                controller.subscribe(room)
                val first = withTimeout(2000) { rpc.calls.receive() }
                first.release.complete(Unit)
                withTimeout(2000) { controller.awaitSubscription(room) }
                source.value = SessionId(byteArrayOf(3))
                val next = withTimeout(2000) { rpc.calls.receive() }
                assertEquals(source.value, next.request.sessionId)
                val waiting = async(start = CoroutineStart.UNDISPATCHED) { controller.awaitSubscription(room) }
                assertFalse(waiting.isCompleted, "An acknowledgment for the old session must not satisfy the new one")
                // A duplicate UI subscription must neither cancel recovery nor issue a third RPC.
                controller.subscribe(room)
                next.release.complete(Unit)
                withTimeout(2000) { waiting.await() }
                assertTrue(rpc.calls.tryReceive().isFailure)
            } finally { controller.stop() }
        } }

    @Test(timeout = 15_000)
    fun `unsubscribe supersedes a pending subscribe and survives session replacement`() =
        runTestWithServer(Application::attachTestServices) { server, _ -> withContext(Dispatchers.Default) {
            val store = InMemoryStoreDelegate()
            val sessions = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), store)
            val subscriptions = SubscriptionStoreImpl(store)
            sessions.prepare(); subscriptions.prepare(); store.createStores()
            val source = MutableStateFlow<SessionId?>(SessionId(byteArrayOf(4)))
            val rpc = GatedClient(server.rpcClient)
            val controller = SubscriptionController(rpc, subscriptions, sessions, source)
            val room = RoomId(byteArrayOf(5))
            controller.startWatchingSubscriptions()
            try {
                controller.subscribe(room)
                val stale = withTimeout(2000) { rpc.calls.receive() }
                controller.unsubscribe(room)
                val remove = withTimeout(2000) { rpc.calls.receive() }
                assertTrue(remove.request.kind is SubscriptionRequest.OneOfKind.unsubscribe)
                source.value = SessionId(byteArrayOf(6))
                val restored = withTimeout(2000) { rpc.calls.receive() }
                assertTrue(restored.request.kind is SubscriptionRequest.OneOfKind.unsubscribe)
                assertEquals(source.value, restored.request.sessionId)
                stale.release.complete(Unit); remove.release.complete(Unit); restored.release.complete(Unit)
                withTimeout(2000) {
                    while (subscriptions.getSubscription(room) != null) delay(10)
                }
                assertNull(withTimeoutOrNull(100) { controller.awaitSubscription(room) })
            } finally { controller.stop() }
        } }
}
