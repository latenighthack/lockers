package com.latenighthack.lockers.server

import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.Database
import com.latenighthack.lockers.common.SessionSigning
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.agents.LockerAgentRegistry
import com.latenighthack.lockers.server.claim.PgTestGate
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.session.v1.SessionGatewayService
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.sql.DriverManager
import kotlinx.coroutines.*
import kotlin.random.Random
import kotlin.test.*

class SubscriptionLockOrderTest {
    @Test fun `signed monolith subscription does not invert dispatcher and database ownership`() = runBlocking {
        val db = ServerStorage.inMemory(); db.open()
        try { exercise(db, db) } finally { db.close() }
    }

    @Test fun `signed PostgreSQL subscription does not invert dispatcher and room transaction across handles`() = runBlocking {
        val base = PgTestGate.urlOrSkip(); val schema = "subscription_order_${System.nanoTime()}"
        DriverManager.getConnection(base).use { it.createStatement().use { statement -> statement.execute("CREATE SCHEMA $schema") } }
        val location = base + (if (base.contains('?')) "&" else "?") + "currentSchema=$schema"
        val writer = ServerStorage.postgres(location); val subscriber = ServerStorage.postgres(location)
        try { writer.open(); subscriber.open(); exercise(writer, subscriber) }
        finally {
            writer.close(); subscriber.close()
            DriverManager.getConnection(base).use { it.createStatement().use { statement -> statement.execute("DROP SCHEMA $schema CASCADE") } }
        }
    }

    private suspend fun exercise(writerDb: Database, subscriptionDb: Database) = coroutineScope {
        val room = RoomId(byteArrayOf(2)); val sid = SessionId(byteArrayOf(1))
        val identity = Secp256r1KeyPair.generate(); val roomKey = Secp256r1KeyPair.generate()
        val sessions = SessionStoreImpl(subscriptionDb)
        sessions.updateSession(ServerSession(sessionId = ServerSessionId(sid.rawValue), authorizedPublicKey = identity.publicKey.encode()))
        val writerHasDispatcher = CompletableDeferred<Unit>(); val allowWriterTransaction = CompletableDeferred<Unit>()
        val subscriptionHasTransaction = CompletableDeferred<Unit>()
        val realLocks = LockStoreImpl(writerDb)
        val locks = object : LockStore by realLocks {
            override suspend fun <T> atomic(roomId: ServerRoomId, block: suspend () -> T): T {
                writerHasDispatcher.complete(Unit)
                allowWriterTransaction.await()
                return realLocks.atomic(roomId, block)
            }
        }
        val realSubscriptions = SubscriptionStoreImpl(subscriptionDb)
        val subscriptions = object : SubscriptionStore by realSubscriptions {
            override suspend fun <T> withIntent(sessionId: ServerSessionId, roomId: ServerRoomId, revision: Long, subscribed: Boolean, mutation: suspend () -> T): SubscriptionIntentResult<T> =
                realSubscriptions.withIntent(sessionId, roomId, revision, subscribed) {
                    subscriptionHasTransaction.complete(Unit)
                    mutation()
                }
        }
        val service = RoomServiceImpl(subscriptions, LockerStoreImpl(writerDb), locks,
            object : SessionGatewayDiscovery { override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = null },
            LocalRoomOwnership(), LockerAgentRegistry.None, SimpleMeterRegistry(), LockersConfig.defaults().copy(deliveryWorkerEnabled = false))
        val rpc = LocalRoomServiceRpc(AuthorizedRoomServer(service, SessionProofVerifier(subscriptionDb, sessions)))
        val request = SubscriptionRequest { sessionId = sid; roomId = room; intentRevision = 1; kind.subscribe {} }
        val issued = System.currentTimeMillis(); val nonce = Random.nextBytes(32)
        val proof = SessionProof(issued, nonce, Signature(signingVersion = 2, signature = identity.privateKey.sign(
            SessionSigning.context(SessionSigning.SUBSCRIPTION, sid, SHA256.digest(request.toByteArray()), issued, nonce))))
        val write = async(Dispatchers.Default) { rpc.lockLocker(LockLockerRequest(roomId = room,
            grant = LockGrant(LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM), publicKeyOf(roomKey.publicKey.encode())))) }
        var subscribe: Deferred<SubscriptionResponse>? = null
        try {
            withTimeout(5_000) { writerHasDispatcher.await() }
            subscribe = async(Dispatchers.Default) { rpc.subscription(request.copy(proof = proof)) }
            withTimeout(5_000) { subscriptionHasTransaction.await() }
            // Both owners are now known: writer has the room dispatcher; subscription has its DB transaction.
            allowWriterTransaction.complete(Unit)
            withTimeout(2_000) {
                assertEquals(SubscriptionResponse.Result.OK, subscribe.await().result)
                assertEquals(LockLockerResponse.Result.OK, write.await().result)
            }
            assertEquals(listOf(ServerRoomId(room.rawValue)), realSubscriptions.getAllSubscriptions(ServerSessionId(sid.rawValue)))
            assertEquals(1L, realSubscriptions.withIntent(ServerSessionId(sid.rawValue), ServerRoomId(room.rawValue), 0, false) { error("stale effect") }.currentRevision)
        } finally {
            allowWriterTransaction.complete(Unit)
            subscribe?.cancelAndJoin(); write.cancelAndJoin(); service.closeAndJoin()
        }
    }
}
