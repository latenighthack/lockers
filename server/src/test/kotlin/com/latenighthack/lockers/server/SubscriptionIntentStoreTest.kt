package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.net.RpcResponseException
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import kotlinx.coroutines.*
import java.io.File
import kotlin.test.*

class SubscriptionIntentStoreTest {
    @Test fun `unsubscribe history rejects stale and conflicting generations but exact replay is idempotent`() = runBlocking {
        val db = ServerStorage.inMemory(); db.open(); val store = SubscriptionStoreImpl(db)
        val session = ServerSessionId(byteArrayOf(1)); val room = ServerRoomId(byteArrayOf(2))
        suspend fun update(revision: Long, subscribed: Boolean) = store.withIntent(session, room, revision, subscribed) {
            if (subscribed) store.addSubscription(session, room) else store.removeSubscription(session, room)
            "applied"
        }
        try {
            assertEquals(0L, update(0, true).currentRevision)
            assertEquals(1L, update(1, true).currentRevision)
            assertEquals(2L, update(2, false).currentRevision)
            assertTrue(update(1, true).stale); assertTrue(update(0, true).stale)
            assertTrue(store.getAllSubscriptions(session).isEmpty())
            assertEquals("applied", update(2, false).value)
            assertEquals(Codes.FAILED_PRECONDITION, assertFailsWith<RpcResponseException> { update(2, true) }.code)
            assertEquals(Codes.INVALID_ARGUMENT, assertFailsWith<RpcResponseException> { update(-1, true) }.code)
            assertTrue(store.observeIntent(session, room, 2) { error("unsubscribed page must not run") }.stale)
            assertEquals(3L, update(3, true).currentRevision)
            assertEquals("page", store.observeIntent(session, room, 3) { "page" }.value)
            assertTrue(store.observeIntent(session, room, 2) { error("stale page must not run") }.stale)
        } finally { db.close() }
    }
    @Test fun `source failure rolls back its new revision and tombstones count against finite namespace until cleanup`() = runBlocking {
        val db = ServerStorage.inMemory(); db.open()
        val store = SubscriptionStoreImpl(db, ServerResourceLimits(maxSubscriptionIntents = 1, maxSubscriptionIntentsPerSession = 1))
        val sid = ServerSessionId(byteArrayOf(1)); val room = ServerRoomId(byteArrayOf(2))
        try {
            assertFailsWith<IllegalStateException> { store.withIntent(sid, room, 10, true) { store.addSubscription(sid, room); error("source rollback") } }
            assertTrue(store.getAllSubscriptions(sid).isEmpty())
            assertFalse(store.withIntent(sid, room, 1, false) { Unit }.stale)
            val failure = assertFailsWith<RpcResponseException> { store.withIntent(sid, ServerRoomId(byteArrayOf(3)), 2, true) { error("capacity must prevent mutation") } }
            assertEquals(Codes.FAILED_PRECONDITION, failure.code)
            assertTrue(store.withIntent(sid, room, 0, true) { error("tombstone must remain") }.stale)
            SubscriptionIntents(db, ServerResourceLimits()).clearForSession(sid)
            assertFalse(store.withIntent(sid, ServerRoomId(byteArrayOf(3)), 2, false) { Unit }.stale)
        } finally { db.close() }
    }
    @Test fun `intent identity bytes are frozen before waiting for a transaction owner`() = runBlocking {
        val db = ServerStorage.inMemory(); db.open(); val ledger = SubscriptionIntents(db, ServerResourceLimits())
        val rawSid = byteArrayOf(1); val rawRoom = byteArrayOf(2)
        val sid = ServerSessionId(rawSid); val room = ServerRoomId(rawRoom)
        val held = CompletableDeferred<Unit>(); val release = CompletableDeferred<Unit>()
        val owner = async { db.transaction("lockers.session-authority") { held.complete(Unit); release.await() } }
        held.await()
        try {
            val queued = async(start = CoroutineStart.UNDISPATCHED) { ledger.apply(sid, room, 1, true) { Unit } }
            rawSid[0] = 9; rawRoom[0] = 8; release.complete(Unit); owner.await(); queued.await()
            assertTrue(ledger.apply(ServerSessionId(byteArrayOf(1)), ServerRoomId(byteArrayOf(2)), 0, true) { error("original frozen authority exists") }.stale)
            assertFalse(ledger.apply(sid, room, 0, true) { Unit }.stale)
        } finally { release.complete(Unit); owner.await(); db.close() }
    }
    @Test fun `V4 SQLite subscriptions migrate without losing rows and intent revisions survive reopen`() = runBlocking {
        val file = File.createTempFile("subscription-intent", ".db")
        val modern = ServerStorage.configuration("subscription-${file.name}")
        val old = modern.copy(version = 4, stores = ServerStorage.definitionsV4.map { it.declaration }, migrations = modern.migrations.dropLast(1))
        var db = createDatabase(old, file.absolutePath); db.open()
        val sid = ServerSessionId(byteArrayOf(1)); val room = ServerRoomId(byteArrayOf(2))
        try {
            val historical = object : Store<ServerSubscription>(db, SubscriptionStoreImplDefinitionV1) {
                suspend fun insert() = save(ServerSubscription(sid, room))
            }
            historical.prepare(); historical.insert(); db.close()
            db = createDatabase(modern, file.absolutePath); db.open()
            var store = SubscriptionStoreImpl(db)
            assertEquals(listOf(room), store.getAllSubscriptions(sid))
            store.withIntent(sid, room, 7, false) { store.removeSubscription(sid, room) }
            db.close(); db = createDatabase(modern, file.absolutePath); db.open(); store = SubscriptionStoreImpl(db)
            assertTrue(store.withIntent(sid, room, 6, true) { error("reopened stale effect") }.stale)
            assertTrue(store.getAllSubscriptions(sid).isEmpty())
            assertEquals(7L, store.withIntent(sid, room, 7, false) { Unit }.currentRevision)
        } finally { db.close(); file.delete() }
    }
}
