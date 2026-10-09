package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.net.RpcResponseException
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.lockers.server.claim.PgTestGate
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import java.sql.DriverManager
import kotlinx.coroutines.*
import kotlin.test.*

class SubscriptionIntentPgTest {
    @Test fun `two PostgreSQL handles serialize intent order and lifetime namespace admission`() = runBlocking {
        val base = PgTestGate.urlOrSkip(); val schema = "subscription_intent_${System.nanoTime()}"
        DriverManager.getConnection(base).use { it.createStatement().use { statement -> statement.execute("CREATE SCHEMA $schema") } }
        val location = base + (if (base.contains('?')) "&" else "?") + "currentSchema=$schema"
        val a = ServerStorage.postgres(location); val b = ServerStorage.postgres(location)
        try {
            a.open(); b.open()
            val limits = ServerResourceLimits(maxSubscriptionIntents = 2, maxSubscriptionIntentsPerSession = 2)
            val stores = listOf(SubscriptionStoreImpl(a, limits), SubscriptionStoreImpl(b, limits))
            val sid = ServerSessionId(byteArrayOf(1)); val room = ServerRoomId(byteArrayOf(2))
            coroutineScope { (1..20).map { index -> async(Dispatchers.Default) {
                val store = stores[index % 2]; val subscribe = index % 2 == 1
                store.withIntent(sid, room, if (subscribe) 1 else 2, subscribe) {
                    if (subscribe) store.addSubscription(sid, room) else store.removeSubscription(sid, room)
                }
            } }.awaitAll() }
            assertTrue(stores[0].getAllSubscriptions(sid).isEmpty())
            assertEquals(2L, stores[1].withIntent(sid, room, 1, true) { error("cross-handle stale mutation") }.currentRevision)
            val accepted = coroutineScope { (3..12).map { index -> async(Dispatchers.Default) {
                try { stores[index % 2].withIntent(sid, ServerRoomId(byteArrayOf(index.toByte())), 3, false) { Unit }; true }
                catch (failure: RpcResponseException) { assertEquals(Codes.FAILED_PRECONDITION, failure.code); false }
            } }.awaitAll() }
            assertEquals(1, accepted.count { it }, "unsubscribed identities reserve lifetime capacity across both SQL handles")
        } finally {
            a.close(); b.close()
            DriverManager.getConnection(base).use { it.createStatement().use { statement -> statement.execute("DROP SCHEMA $schema CASCADE") } }
        }
    }
}
