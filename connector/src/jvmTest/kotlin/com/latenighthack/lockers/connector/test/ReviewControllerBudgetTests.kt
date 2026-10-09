package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.ktbuf.rpc.*
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.test.runTest
import kotlin.test.*

class ReviewControllerBudgetTests {
    @Test fun `subscription observes exhausted transport retry budget without treating it as caller cancellation`() = runTest {
        val db = ConnectorStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db).also { it.prepare() }
        val subscriptions = SubscriptionStoreImpl(db).also { it.prepare() }
        var calls = 0
        val controller = SubscriptionController(ReviewRpc { _, _ -> requireNotNull(repeatWithBackoff<ByteArray>(retryLimit = 2, jitter = 0f) {
            calls++; throw RpcResponseException("test", "POST", Codes.RESOURCE_EXHAUSTED, "temporary quota")
        }) }, subscriptions, sessions, MutableStateFlow(SessionId(byteArrayOf(1))), coroutineContext = coroutineContext)
        try {
            controller.subscribe(RoomId(byteArrayOf(2)))
            assertFailsWith<RetryLimitExceeded> { withTimeout(5_000) { controller.awaitSubscription(RoomId(byteArrayOf(2))) } }
            assertEquals(3, calls)
            assertIs<RetryLimitExceeded>(controller.failures.value.values.single())
            assertTrue(subscriptions.getAllSubscriptions().single().isPendingAdd)
        } finally { controller.closeAndJoin(); db.close() }
    }
    @Test fun `push observes exhausted transport retry budget and retains its durable intent`() = runTest {
        val db = ConnectorStorage.inMemory(); db.open()
        val store = PushRegistrationStoreImpl(db).also { it.prepare() }; var calls = 0
        val controller = PushRegistrationController(ReviewRpc { _, _ -> requireNotNull(repeatWithBackoff<ByteArray>(retryLimit = 2, jitter = 0f) {
            calls++; throw RpcResponseException("test", "POST", Codes.RESOURCE_EXHAUSTED, "temporary quota")
        }) }, store, MutableStateFlow(SessionId(byteArrayOf(1))), coroutineContext = coroutineContext)
        try {
            controller.register(PushRegistrations.fcm("token"))
            assertFailsWith<RetryLimitExceeded> { withTimeout(5_000) { controller.awaitRegistered(PushBackendType.FCM) } }
            assertEquals(3, calls)
            assertIs<RetryLimitExceeded>(controller.registrations.value.getValue(PushBackendType.FCM).failure)
            assertTrue(store.getAllIntents().single().pending)
        } finally { controller.closeAndJoin(); db.close() }
    }
}
