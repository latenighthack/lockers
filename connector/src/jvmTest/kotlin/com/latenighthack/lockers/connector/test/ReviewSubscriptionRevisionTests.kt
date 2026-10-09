package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.room.v1.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.test.runTest
import kotlin.test.*

class ReviewSubscriptionRevisionTests {
    @Test fun `subscription removal revision survives same session controller replacement`() = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db).also { it.prepare() }
        val store = SubscriptionStoreImpl(db).also { it.prepare() }; val room = RoomId(byteArrayOf(2))
        val requests = java.util.concurrent.CopyOnWriteArrayList<SubscriptionRequest>()
        val rpc = ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(subscriptionRevisions = true).toByteArray()
            "Subscription" -> { val request = SubscriptionRequest.fromByteArray(bytes); requests += request; SubscriptionResponse(currentRevision = request.intentRevision).toByteArray() }
            else -> error(method.methodName)
        } }
        fun client() = SubscriptionController(rpc, store, sessions, MutableStateFlow(SessionId(byteArrayOf(1))))
        var controller = client()
        try {
            controller.subscribe(room); withTimeout(2_000) { controller.awaitSubscription(room) }
            assertEquals(1L, requests.single().intentRevision)
            controller.unsubscribe(room); withTimeout(2_000) { while (store.getAllSubscriptions().isNotEmpty()) delay(10) }
            controller.closeAndJoin(); controller = client()
            controller.subscribe(room); withTimeout(2_000) { controller.awaitSubscription(room) }
            assertEquals(listOf(1L, 2L, 3L), requests.map { it.intentRevision })
        } finally { controller.closeAndJoin(); db.close() }
    }
    @Test fun `current stale intent advances durable floor and retries with the same desired kind`() = runTest {
        val db = ConnectorStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db).also { it.prepare() }
        val store = SubscriptionStoreImpl(db).also { it.prepare() }; val room = RoomId(byteArrayOf(2))
        val revisions = mutableListOf<Long>()
        val controller = SubscriptionController(ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(subscriptionRevisions = true).toByteArray()
            "Subscription" -> { val request = SubscriptionRequest.fromByteArray(bytes); revisions += request.intentRevision
                if (request.intentRevision <= 5) SubscriptionResponse(SubscriptionResponse.Result.STALE_INTENT, 5).toByteArray()
                else SubscriptionResponse(currentRevision = request.intentRevision).toByteArray() }
            else -> error(method.methodName)
        } }, store, sessions, MutableStateFlow(SessionId(byteArrayOf(1))), coroutineContext = coroutineContext)
        try {
            controller.subscribe(room); withTimeout(2_000) { controller.awaitSubscription(room) }
            assertEquals(listOf(1L, 6L), revisions)
            assertTrue(controller.failures.value.isEmpty())
        } finally { controller.closeAndJoin(); db.close() }
    }
    @Test fun `obsolete stale response never advances a newer desired intent`() = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db).also { it.prepare() }
        val store = SubscriptionStoreImpl(db).also { it.prepare() }; val room = RoomId(byteArrayOf(2))
        val entered = CompletableDeferred<Unit>(); val release = CompletableDeferred<Unit>(); val returned = CompletableDeferred<Unit>()
        val revisions = java.util.concurrent.CopyOnWriteArrayList<Long>()
        val controller = SubscriptionController(ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(subscriptionRevisions = true).toByteArray()
            "Subscription" -> { val request = SubscriptionRequest.fromByteArray(bytes); revisions += request.intentRevision
                if (request.intentRevision == 1L) {
                    entered.complete(Unit); withContext(NonCancellable) { release.await() }; returned.complete(Unit)
                    SubscriptionResponse(SubscriptionResponse.Result.STALE_INTENT, 100).toByteArray()
                } else SubscriptionResponse(currentRevision = request.intentRevision).toByteArray() }
            else -> error(method.methodName)
        } }, store, sessions, MutableStateFlow(SessionId(byteArrayOf(1))))
        try {
            controller.subscribe(room); withTimeout(2_000) { entered.await() }; controller.unsubscribe(room)
            withTimeout(2_000) { while (store.getAllSubscriptions().isNotEmpty()) delay(10) }
            release.complete(Unit); returned.await(); delay(50)
            assertEquals(2L, store.intentRevision(room)!!.revision)
            controller.subscribe(room); withTimeout(2_000) { controller.awaitSubscription(room) }
            assertEquals(listOf(1L, 2L, 3L), revisions)
        } finally { release.complete(Unit); controller.closeAndJoin(); db.close() }
    }
    @Test fun `persistent floor conflicts terminate after bounded guarded repair`() = runTest {
        val db = ConnectorStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db).also { it.prepare() }
        val store = SubscriptionStoreImpl(db).also { it.prepare() }; var calls = 0
        val controller = SubscriptionController(ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(subscriptionRevisions = true).toByteArray()
            "Subscription" -> { calls++; val request = SubscriptionRequest.fromByteArray(bytes)
                SubscriptionResponse(SubscriptionResponse.Result.STALE_INTENT, request.intentRevision + 1).toByteArray() }
            else -> error(method.methodName)
        } }, store, sessions, MutableStateFlow(SessionId(byteArrayOf(1))), coroutineContext = coroutineContext)
        try {
            controller.subscribe(RoomId(byteArrayOf(2)))
            assertFailsWith<SubscriptionRevisionConflictException> { withTimeout(2_000) { controller.awaitSubscription(RoomId(byteArrayOf(2))) } }
            assertEquals(4, calls); assertTrue(store.getAllSubscriptions().single().isPendingAdd)
        } finally { controller.closeAndJoin(); db.close() }
    }
    @Test fun `unsupported ordering fails observably before subscription RPC and retains desired intent`() = runTest {
        val db = ConnectorStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db).also { it.prepare() }
        val store = SubscriptionStoreImpl(db).also { it.prepare() }; var calls = 0
        val controller = SubscriptionController(ReviewRpc { method, _ ->
            assertEquals("Capabilities", method.methodName); calls++; CapabilitiesResponse(subscriptionRevisions = false).toByteArray()
        }, store, sessions, MutableStateFlow(SessionId(byteArrayOf(1))), coroutineContext = coroutineContext)
        try {
            controller.subscribe(RoomId(byteArrayOf(2)))
            assertFailsWith<SubscriptionOrderingUnsupportedException> { withTimeout(2_000) { controller.awaitSubscription(RoomId(byteArrayOf(2))) } }
            assertEquals(1, calls); assertTrue(store.getAllSubscriptions().single().isPendingAdd)
            assertIs<SubscriptionOrderingUnsupportedException>(controller.failures.value.values.single())
        } finally { controller.closeAndJoin(); db.close() }
    }
    @Test fun `bounded removal history rejects admission atomically without resetting old revisions`() = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        val store = SubscriptionStoreImpl(db, ConnectorRetentionPolicy(maxSubscriptions = 1, maxSubscriptionHistories = 2))
        try {
            for (n in 1..2) {
                val room = RoomId(byteArrayOf(n.toByte())); assertEquals(1L, store.commitIntent(room, true))
                assertEquals(2L, store.commitIntent(room, false)); store.deleteSubscription(room)
            }
            assertFailsWith<SubscriptionHistoryCapacityException> { store.commitIntent(RoomId(byteArrayOf(3)), true) }
            assertTrue(store.getAllSubscriptions().isEmpty())
            assertEquals(2L, store.intentRevision(RoomId(byteArrayOf(1)))!!.revision)
            assertEquals(3L, store.commitIntent(RoomId(byteArrayOf(1)), true))
        } finally { db.close() }
    }

}
