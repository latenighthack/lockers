package com.latenighthack.lockers.connector.test

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.connector.storage.v1.StoredSubscription
import com.latenighthack.lockers.room.v1.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlin.test.*

class ReviewSubscriptionStartupCasTests {
    @Test fun `startup rereads current durable intent instead of resurrecting a gated stale row snapshot`() = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db).also { it.prepare() }
        val actual = SubscriptionStoreImpl(db); val room = RoomId(byteArrayOf(2)); actual.commitIntent(room, true)
        val entered = CompletableDeferred<Unit>(); val release = CompletableDeferred<Unit>()
        val frames = java.util.concurrent.CopyOnWriteArrayList<SubscriptionRequest>()
        val controller = SubscriptionController(ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(subscriptionRevisions = true).toByteArray()
            "Subscription" -> { val frame = SubscriptionRequest.fromByteArray(bytes); frames += frame
                SubscriptionResponse(currentRevision = frame.intentRevision).toByteArray() }
            else -> error(method.methodName)
        } }, object : SubscriptionStore by actual {
            override suspend fun getAllSubscriptions(): List<StoredSubscription> {
                val snapshot = actual.getAllSubscriptions(); entered.complete(Unit); release.await(); return snapshot
            }
        }, sessions, MutableStateFlow(SessionId(byteArrayOf(1))))
        try {
            val startup = async { controller.startWatchingSubscriptions() }; entered.await()
            assertEquals(2L, actual.commitIntent(room, false))
            release.complete(Unit); startup.await()
            withTimeout(2_000) { while (frames.isEmpty()) delay(10) }
            assertEquals(2L, frames.single().intentRevision)
            assertIs<SubscriptionRequest.OneOfKind.unsubscribe>(frames.single().kind)
            assertEquals(2L, actual.intentRevision(room)!!.revision); assertFalse(actual.intentRevision(room)!!.subscribed)
        } finally { release.complete(Unit); controller.closeAndJoin(); db.close() }
    }
    @Test fun `public revision encoder rejects invalid identities and nonpositive counters`() {
        for (value in listOf(SubscriptionIntentRevision(byteArrayOf(), 1, true), SubscriptionIntentRevision(ByteArray(129), 1, true),
            SubscriptionIntentRevision(byteArrayOf(1), 0, true), SubscriptionIntentRevision(byteArrayOf(1), -1, true))) {
            assertFailsWith<IllegalArgumentException> { SubscriptionIntentRevisionDefinitionV1.encodePayload(value) }
        }
        val valid = SubscriptionIntentRevision(byteArrayOf(1), Long.MAX_VALUE, false)
        val decoded = SubscriptionIntentRevisionDefinitionV1.decode(SubscriptionIntentRevisionDefinitionV1.encodePayload(valid))
        assertContentEquals(valid.room, decoded.room); assertEquals(valid.revision, decoded.revision); assertEquals(valid.subscribed, decoded.subscribed)
    }
}
