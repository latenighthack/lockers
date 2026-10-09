package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.connector.storage.v1.StoredSubscription
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.*

class ReviewSubscriptionFailureTests {
    @Test fun `retiring subscription effects remain bounded across session generations`() = runBlocking { withContext(Dispatchers.Default) {
        val db = ConnectorStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db).also { it.prepare() }
        val subscriptions = SubscriptionStoreImpl(db, ConnectorRetentionPolicy(maxSubscriptions = 2)).also { it.prepare() }
        val source = MutableStateFlow<SessionId?>(SessionId(byteArrayOf(1)))
        val entered = kotlinx.coroutines.channels.Channel<Int>(32)
        val release = CompletableDeferred<Unit>()
        val active = AtomicInteger(); val peak = AtomicInteger(); val total = AtomicInteger()
        val heldRoom = RoomId(byteArrayOf(2)); val healthyRoom = RoomId(byteArrayOf(3))
        val controller = SubscriptionController(ReviewRpc { _, bytes ->
            val request = SubscriptionRequest.fromByteArray(bytes)
            if (request.roomId!!.rawValue[0] == 2.toByte()) {
                val count = active.incrementAndGet(); peak.updateAndGet { maxOf(it, count) }; total.incrementAndGet()
                entered.send(request.sessionId!!.rawValue[0].toInt())
                try { withContext(NonCancellable) { release.await() } } finally { active.decrementAndGet() }
            }
            SubscriptionResponse().toByteArray()
        }, subscriptions, sessions, source, coroutineContext = currentCoroutineContext())
        try {
            controller.subscribe(heldRoom); controller.subscribe(healthyRoom)
            assertEquals(1, withTimeout(2_000) { entered.receive() })
            for (generation in 2..20) {
                source.value = SessionId(byteArrayOf(generation.toByte()))
                withTimeout(2_000) { controller.awaitSubscription(healthyRoom) }
                if (generation == 2) assertEquals(2, withTimeout(2_000) { entered.receive() })
                delay(10) // Healthy confirmation is the processed-session barrier; queued held-room work is conflated.
            }
            assertEquals(2, subscriptions.getAllSubscriptions().size)
            assertTrue(peak.get() <= 2, "one room retained ${active.get()} RPC effects (peak=${peak.get()}, total=${total.get()}) despite only two persisted intents")
            release.complete(Unit)
            withTimeout(2_000) { controller.awaitSubscription(heldRoom) }
            assertEquals(20, withTimeout(2_000) { entered.receive() }, "completion must run only the latest deferred session")
        } finally { release.complete(Unit); controller.closeAndJoin(); assertEquals(0, active.get()); db.close() }
    } }

    @Test fun `removed room identities retain their global work budget until actual cleanup completes`() = runBlocking { withContext(Dispatchers.Default) {
        val db = ConnectorStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db).also { it.prepare() }
        val subscriptions = SubscriptionStoreImpl(db, ConnectorRetentionPolicy(maxSubscriptions = 1)).also { it.prepare() }
        val release = CompletableDeferred<Unit>(); val entered = kotlinx.coroutines.channels.Channel<Unit>(2)
        val active = AtomicInteger(); val peak = AtomicInteger()
        val controller = SubscriptionController(ReviewRpc { _, bytes ->
            val request = SubscriptionRequest.fromByteArray(bytes)
            if (request.kind is SubscriptionRequest.OneOfKind.subscribe) {
                val count = active.incrementAndGet(); peak.updateAndGet { maxOf(it, count) }; entered.send(Unit)
                try { withContext(NonCancellable) { release.await() } } finally { active.decrementAndGet() }
            }
            SubscriptionResponse().toByteArray()
        }, subscriptions, sessions, MutableStateFlow(SessionId(byteArrayOf(1))), coroutineContext = currentCoroutineContext())
        try {
            val first = RoomId(byteArrayOf(2)); val second = RoomId(byteArrayOf(3))
            controller.subscribe(first); withTimeout(1_500) { entered.receive() }; controller.unsubscribe(first)
            withTimeout(1_500) { while (subscriptions.getAllSubscriptions().isNotEmpty()) delay(10) }
            controller.subscribe(second); withTimeout(1_500) { entered.receive() }; controller.unsubscribe(second)
            val removed = withTimeoutOrNull(200) { while (subscriptions.getAllSubscriptions().isNotEmpty()) delay(10); true }
            assertNull(removed, "retired deleted-room work must still consume the finite global effect budget")
            assertTrue(subscriptions.getAllSubscriptions().single().isPendingRemove)
            assertEquals(2, controller.work.value.occupiedEffects)
            assertEquals(2, controller.work.value.retiringEffects)
            assertEquals(1, controller.work.value.queuedRooms.size)
            assertContentEquals(second.rawValue, controller.work.value.queuedRooms.single().rawValue)
            controller.work.value.queuedRooms.single().rawValue.fill(0)
            assertContentEquals(second.rawValue, controller.work.value.queuedRooms.single().rawValue)
            assertFailsWith<SubscriptionCapacityException> { controller.subscribe(RoomId(byteArrayOf(4))) }
            assertEquals(2, peak.get())
            release.complete(Unit)
            withTimeout(1_500) { while (subscriptions.getAllSubscriptions().isNotEmpty()) delay(10) }
            controller.subscribe(RoomId(byteArrayOf(4))); withTimeout(1_500) { controller.awaitSubscription(RoomId(byteArrayOf(4))) }
        } finally { release.complete(Unit); controller.closeAndJoin(); assertEquals(0, active.get()); db.close() }
    } }

    @Test fun `one permanent subscription failure is observable and does not stop another room`() = runBlocking { withContext(Dispatchers.Default) {
        val db = ConnectorStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db).also { it.prepare() }
        val subscriptions = SubscriptionStoreImpl(db).also { it.prepare() }
        val session = MutableStateFlow<SessionId?>(SessionId(byteArrayOf(1)))
        val calls = ConcurrentHashMap<Int, AtomicInteger>()
        var reject = true
        val controller = SubscriptionController(ReviewRpc { _, bytes ->
            val request = SubscriptionRequest.fromByteArray(bytes); val room = request.roomId!!.rawValue[0].toInt()
            calls.computeIfAbsent(room) { AtomicInteger() }.incrementAndGet()
            if (room == 2 && reject) throw RpcResponseException("test", "POST", Codes.FAILED_PRECONDITION, "permanent room namespace ceiling")
            SubscriptionResponse().toByteArray()
        }, subscriptions, sessions, session, coroutineContext = currentCoroutineContext())
        try {
            controller.startWatchingSubscriptions(); controller.subscribe(RoomId(byteArrayOf(2)))
            val failure = assertFailsWith<RpcResponseException> { withTimeout(1_500) { controller.awaitSubscription(RoomId(byteArrayOf(2))) } }
            assertEquals(Codes.FAILED_PRECONDITION, failure.code); assertEquals(1, calls[2]!!.get())
            controller.subscribe(RoomId(byteArrayOf(3))); withTimeout(1_500) { controller.awaitSubscription(RoomId(byteArrayOf(3))) }
            assertEquals(1, calls[3]!!.get())
            reject = false
            controller.subscribe(RoomId(byteArrayOf(2))) // A new explicit decision retries the failed intent.
            withTimeout(1_500) { controller.awaitSubscription(RoomId(byteArrayOf(2))) }
            assertEquals(2, calls[2]!!.get())
        } finally { controller.closeAndJoin(); db.close() }
    } }

    @Test fun `temporary subscription quota retries and old permanent failure cannot poison a replacement session`() = runBlocking { withContext(Dispatchers.Default) {
        val db = ConnectorStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db).also { it.prepare() }
        val subscriptions = SubscriptionStoreImpl(db).also { it.prepare() }
        val source = MutableStateFlow<SessionId?>(SessionId(byteArrayOf(1)))
        val entered = CompletableDeferred<Unit>(); val release = CompletableDeferred<Unit>(); val staleReturned = CompletableDeferred<Unit>()
        val calls = AtomicInteger()
        val controller = SubscriptionController(ReviewRpc { _, bytes ->
            val request = SubscriptionRequest.fromByteArray(bytes)
            calls.incrementAndGet()
            if (request.sessionId!!.rawValue[0] == 1.toByte()) {
                entered.complete(Unit); withContext(NonCancellable) { release.await() }; staleReturned.complete(Unit)
                throw RpcResponseException("test", "POST", Codes.OUT_OF_RANGE, "old request ceiling")
            }
            if (calls.get() == 2) throw RpcResponseException("test", "POST", Codes.RESOURCE_EXHAUSTED, "temporary quota")
            SubscriptionResponse().toByteArray()
        }, subscriptions, sessions, source, coroutineContext = currentCoroutineContext())
        val room = RoomId(byteArrayOf(2))
        try {
            controller.startWatchingSubscriptions(); controller.subscribe(room); withTimeout(1_500) { entered.await() }
            source.value = SessionId(byteArrayOf(3))
            withTimeout(2_000) { controller.awaitSubscription(room) }
            assertEquals(3, calls.get())
            release.complete(Unit); staleReturned.await(); delay(50)
            withTimeout(1_500) { controller.awaitSubscription(room) }
            assertEquals(3, calls.get())
        } finally { release.complete(Unit); controller.closeAndJoin(); db.close() }
    } }
    @Test fun `startup storage failure remains observable instead of leaving started without workers`() = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db).also { it.prepare() }
        val actual = SubscriptionStoreImpl(db).also { it.prepare() }
        val failure = IllegalStateException("subscription read failed")
        val controller = SubscriptionController(ReviewRpc { _, _ -> error("startup must not reach network") }, object : SubscriptionStore by actual {
            override suspend fun getAllSubscriptions(): List<StoredSubscription> = throw failure
        }, sessions, MutableStateFlow(SessionId(byteArrayOf(1))))
        try {
            assertEquals(failure.message, assertFailsWith<IllegalStateException> { controller.startWatchingSubscriptions() }.message)
            assertEquals(failure.message, assertFailsWith<IllegalStateException> { withTimeout(1_500) { controller.awaitSubscription(RoomId(byteArrayOf(2))) } }.message)
            assertSame(failure, controller.failure.value)
            assertEquals(failure.message, assertFailsWith<IllegalStateException> { controller.startWatchingSubscriptions() }.message)
        } finally { controller.closeAndJoin(); db.close() }
    }

    @Test fun `confirmation storage failure is observable without a child crash and replacement recovers intent`() = runBlocking { withContext(Dispatchers.Default) {
        val db = ConnectorStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db).also { it.prepare() }
        val actual = SubscriptionStoreImpl(db).also { it.prepare() }
        val failure = IllegalStateException("subscription confirmation save failed")
        val uncaught = kotlinx.coroutines.channels.Channel<Throwable>(1)
        val rpc = ReviewRpc { _, _ -> SubscriptionResponse().toByteArray() }
        val source = MutableStateFlow<SessionId?>(SessionId(byteArrayOf(1)))
        val controller = SubscriptionController(rpc, object : SubscriptionStore by actual {
            override suspend fun updateSubscription(subscription: StoredSubscription) {
                if (!subscription.isPendingAdd && !subscription.isPendingRemove) throw failure
                actual.updateSubscription(subscription)
            }
        }, sessions, source, coroutineContext = currentCoroutineContext() + CoroutineExceptionHandler { _, error -> uncaught.trySend(error) })
        val room = RoomId(byteArrayOf(2))
        try {
            controller.startWatchingSubscriptions(); controller.subscribe(room)
            assertEquals(failure.message, assertFailsWith<IllegalStateException> { withTimeout(1_500) { controller.awaitSubscription(room) } }.message)
            assertSame(failure, controller.failures.value[room])
            assertTrue(uncaught.tryReceive().isFailure)
            assertTrue(actual.getSubscription(room)!!.isPendingAdd)
        } finally { controller.closeAndJoin() }
        val replacement = SubscriptionController(rpc, actual, sessions, source, coroutineContext = currentCoroutineContext())
        try {
            replacement.startWatchingSubscriptions(); withTimeout(1_500) { replacement.awaitSubscription(room) }
            assertFalse(actual.getSubscription(room)!!.isPendingAdd)
        } finally { replacement.closeAndJoin(); db.close() }
    } }

    @Test fun `unsubscribe storage failure is observable and pending removal survives replacement`() = runBlocking { withContext(Dispatchers.Default) {
        val db = ConnectorStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db).also { it.prepare() }
        val actual = SubscriptionStoreImpl(db).also { it.prepare() }
        val failure = IllegalStateException("subscription removal failed")
        val uncaught = kotlinx.coroutines.channels.Channel<Throwable>(1)
        val rpc = ReviewRpc { _, _ -> SubscriptionResponse().toByteArray() }
        val source = MutableStateFlow<SessionId?>(SessionId(byteArrayOf(1)))
        val controller = SubscriptionController(rpc, object : SubscriptionStore by actual {
            override suspend fun deleteSubscription(roomId: RoomId) { throw failure }
        }, sessions, source, coroutineContext = currentCoroutineContext() + CoroutineExceptionHandler { _, error -> uncaught.trySend(error) })
        val room = RoomId(byteArrayOf(2))
        try {
            controller.startWatchingSubscriptions(); controller.subscribe(room); withTimeout(1_500) { controller.awaitSubscription(room) }
            controller.unsubscribe(room)
            assertEquals(failure.message, assertFailsWith<IllegalStateException> { withTimeout(1_500) { controller.awaitSubscription(room) } }.message)
            assertSame(failure, controller.failures.value[room])
            assertTrue(uncaught.tryReceive().isFailure)
            assertTrue(actual.getSubscription(room)!!.isPendingRemove)
        } finally { controller.closeAndJoin() }
        val replacement = SubscriptionController(rpc, actual, sessions, source, coroutineContext = currentCoroutineContext())
        try {
            replacement.startWatchingSubscriptions(); withTimeout(1_500) { while (actual.getSubscription(room) != null) delay(10) }
        } finally { replacement.closeAndJoin(); db.close() }
    } }

    @Test fun `new intent persistence rejection fails its caller without retaining a ghost room or stopping healthy work`() = runBlocking { withContext(Dispatchers.Default) {
        val db = ConnectorStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db).also { it.prepare() }
        val actual = SubscriptionStoreImpl(db).also { it.prepare() }
        val failure = IllegalStateException("new intent cannot persist")
        val controller = SubscriptionController(ReviewRpc { _, _ -> SubscriptionResponse().toByteArray() }, object : SubscriptionStore by actual {
            override suspend fun updateSubscription(subscription: StoredSubscription) {
                if (subscription.roomIdRawValue[0] == 2.toByte()) throw failure
                actual.updateSubscription(subscription)
            }
        }, sessions, MutableStateFlow(SessionId(byteArrayOf(1))), coroutineContext = currentCoroutineContext())
        try {
            assertEquals(failure.message, assertFailsWith<IllegalStateException> { withTimeout(1_500) { controller.subscribe(RoomId(byteArrayOf(2))) } }.message)
            assertNull(actual.getSubscription(RoomId(byteArrayOf(2))))
            assertTrue(controller.failures.value.isEmpty()); assertNull(controller.failure.value)
            controller.subscribe(RoomId(byteArrayOf(3))); withTimeout(1_500) { controller.awaitSubscription(RoomId(byteArrayOf(3))) }
        } finally { controller.closeAndJoin(); db.close() }
    } }

    @Test fun `failed room churn stays within finite intent and failure bounds`() = runBlocking { withContext(Dispatchers.Default) {
        val db = ConnectorStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db).also { it.prepare() }
        val store = SubscriptionStoreImpl(db, ConnectorRetentionPolicy(maxSubscriptions = 2)).also { it.prepare() }
        val controller = SubscriptionController(ReviewRpc { _, bytes ->
            val request = SubscriptionRequest.fromByteArray(bytes)
            if (request.kind is SubscriptionRequest.OneOfKind.subscribe) throw RpcResponseException("test", "POST", Codes.FAILED_PRECONDITION, "permanent quota")
            SubscriptionResponse().toByteArray()
        }, store, sessions, MutableStateFlow(SessionId(byteArrayOf(1))), coroutineContext = currentCoroutineContext())
        try {
            repeat(10) { index ->
                val room = RoomId(byteArrayOf((index + 2).toByte()))
                controller.subscribe(room)
                assertFailsWith<RpcResponseException> { withTimeout(1_500) { controller.awaitSubscription(room) } }
                assertEquals(1, controller.failures.value.size)
                controller.unsubscribe(room)
                withTimeout(1_500) { while (store.getSubscription(room) != null || controller.failures.value.isNotEmpty()) delay(10) }
            }
            val first = RoomId(byteArrayOf(20)); val second = RoomId(byteArrayOf(21)); val excess = RoomId(byteArrayOf(22))
            controller.subscribe(first); controller.subscribe(second)
            assertFailsWith<SubscriptionCapacityException> { withTimeout(1_500) { controller.subscribe(excess) } }
            assertNull(store.getSubscription(excess))
            assertEquals(2, store.getAllSubscriptions().size)
            assertTrue(controller.failures.value.size <= 2)
        } finally { controller.closeAndJoin(); db.close() }
    } }

    @Test fun `retained permanent RPC metadata preserves status with finite text and identity bounds`() = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db).also { it.prepare() }
        val store = SubscriptionStoreImpl(db).also { it.prepare() }
        val controller = SubscriptionController(ReviewRpc { _, _ -> throw RpcResponseException("test", "POST", Codes.OUT_OF_RANGE, "x".repeat(10_000)) },
            store, sessions, MutableStateFlow(SessionId(byteArrayOf(1))))
        val room = RoomId(byteArrayOf(2))
        try {
            controller.subscribe(room)
            val failure = assertFailsWith<RpcResponseException> { withTimeout(1_500) { controller.awaitSubscription(room) } }
            assertEquals(Codes.OUT_OF_RANGE, failure.code); assertEquals(2_048, failure.errorMessage.length)
            assertIs<RpcResponseException>(controller.failures.value[room])
            assertFailsWith<IllegalArgumentException> { controller.subscribe(RoomId(ByteArray(129))) }
            assertEquals(1, store.getAllSubscriptions().size)
        } finally { controller.closeAndJoin(); db.close() }
    }

}
