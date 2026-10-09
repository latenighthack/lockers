package com.latenighthack.lockers.server

import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.common.SessionSigning
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.session.v1.*
import kotlinx.coroutines.runBlocking
import kotlin.random.Random
import kotlin.test.*

class SessionRevocationTest {
    @Test fun authenticatedDestroyRemovesAuthorityAndReservesTheId(): Unit = runBlocking {
        val db = ServerStorage.inMemory()
        val core = ServerCore::class.create(LockersConfig.defaults().copy(deliveryWorkerEnabled = false, pushWorkerEnabled = false), db)
        core.setup()
        val component = MonolithComponent(core)
        val key = Secp256r1KeyPair.generate()
        val sid = SessionId(byteArrayOf(41))
        val stored = ServerSession(ServerSessionId(sid.rawValue), byteArrayOf(42), key.publicKey.encode())
        val event = ServerSessionEvent(ServerSessionId(sid.rawValue), ServerRoomId(byteArrayOf(43)), ServerEventId(byteArrayOf(44)), byteArrayOf(45))
        core.sessionStore.updateSession(stored)
        core.subscriptionStore.addSubscription(requireNotNull(stored.sessionId), requireNotNull(event.roomId))
        core.sessionInboxStore.saveEvent(event)
        core.pushStore.savePushInfo(ServerPushInfo(requireNotNull(stored.sessionId)))
        core.pushQueueStore.savePush(ServerPush(ServerPushId(byteArrayOf(46)), requireNotNull(stored.sessionId), 1, byteArrayOf(47)))
        val snapshots = com.latenighthack.lockers.server.services.room.v1.SnapshotStore(db, core.config.resourceLimits)
        val snapshotRoom = RoomId(byteArrayOf(49))
        val page = snapshots.create(1, snapshotRoom, sid, setOf(0), 1, 1, (1..2).map {
            IdentifiedLocker(LockerId(byteArrayOf(it.toByte())), Locker { open { encodedPayload = byteArrayOf(1) } }, 1)
        })
        val request = DestroySessionRequest(sessionId = sid)
        val now = System.currentTimeMillis(); val nonce = Random.nextBytes(32)
        val signed = request.copy(proof = SessionProof(now, nonce, Signature(signature = key.privateKey.sign(
            SessionSigning.context(SessionSigning.DESTROY, sid, SHA256.digest(request.toByteArray()), now, nonce)), signingVersion = 2)))
        try {
            val rpc = LocalSessionServiceRpc(component.sessionServiceModule.server)
            assertFalse(rpc.destroySession(request).result.isOk())
            assertNotNull(core.sessionStore.getSessionById(requireNotNull(stored.sessionId)))
            assertTrue(rpc.destroySession(signed).result.isOk())
            assertFailsWith<com.latenighthack.ktbuf.net.RpcResponseException> {
                snapshots.next(1, snapshotRoom, sid, setOf(0), 1, page.nextPageToken)
            }
            assertNull(core.sessionStore.getSessionById(requireNotNull(stored.sessionId)))
            assertTrue(core.subscriptionStore.getAllSubscriptions(requireNotNull(stored.sessionId)).isEmpty())
            assertTrue(core.sessionInboxStore.getAllEvents(requireNotNull(stored.sessionId)).isEmpty())
            assertNull(core.pushStore.getPushInfo(requireNotNull(stored.sessionId)))
            assertTrue(core.pushQueueStore.getPendingPushes().isEmpty())
            assertFalse(core.sessionStore.createIfAbsent(stored), "Destroyed IDs must never be reused by queued old deliveries")
            assertFalse(rpc.destroySession(signed).result.isOk())
        } finally { component.stop(); db.close() }
    }
    @Test fun destroyedAuthorityOnlyConfirmsFreshDestroyAndSurvivesSqliteReopen(): Unit = runBlocking {
        val file = java.io.File.createTempFile("destroy-authority", ".db")
        val configuration = ServerStorage.configuration(file.name)
        val key = Secp256r1KeyPair.generate()
        val wrong = Secp256r1KeyPair.generate()
        val sid = SessionId(byteArrayOf(51))
        val storedId = ServerSessionId(sid.rawValue)
        suspend fun proof(operation: String, bytes: ByteArray, signer: Secp256r1KeyPair = key): SessionProof {
            val now = System.currentTimeMillis(); val nonce = Random.nextBytes(32)
            return SessionProof(now, nonce, Signature(signature = signer.privateKey.sign(
                SessionSigning.context(operation, sid, SHA256.digest(bytes), now, nonce)), signingVersion = 2))
        }
        fun core(database: com.latenighthack.ktstore.Database) = ServerCore::class.create(
            LockersConfig.defaults().copy(deliveryWorkerEnabled = false, pushWorkerEnabled = false), database)
        val original = DestroySessionRequest(sessionId = sid)
        val signed = original.copy(proof = proof(SessionSigning.DESTROY, original.toByteArray()))
        var db = createDatabase(configuration, file.absolutePath)
        var serverCore = core(db).also { it.setup() }
        var component = MonolithComponent(serverCore)
        try {
            serverCore.sessionStore.updateSession(ServerSession(storedId, byteArrayOf(52), key.publicKey.encode()))
            assertTrue(LocalSessionServiceRpc(component.sessionServiceModule.server).destroySession(signed).result.isOk())
            component.stop(); db.close()
            db = createDatabase(configuration, file.absolutePath)
            serverCore = core(db).also { it.setup() }
            component = MonolithComponent(serverCore)
            val store = serverCore.sessionStore
            val rpc = LocalSessionServiceRpc(component.sessionServiceModule.server)
            assertNull(store.getSessionById(storedId))
            assertTrue(store.isRevoked(storedId))
            assertFalse(rpc.destroySession(signed).result.isOk(), "An already consumed nonce stays rejected")
            assertFalse(rpc.destroySession(original).result.isOk(), "Unsigned terminal queries stay rejected")
            assertFalse(rpc.destroySession(original.copy(proof = proof(SessionSigning.DESTROY, original.toByteArray(), wrong))).result.isOk())
            val subscription = com.latenighthack.lockers.room.v1.SubscriptionRequest(sessionId = sid, roomId = RoomId(byteArrayOf(53)))
            val accepted = serverCore.sessionProofVerifier.authorize(SessionSigning.SUBSCRIPTION, sid,
                proof(SessionSigning.SUBSCRIPTION, subscription.toByteArray()), subscription.toByteArray(), { false }) { true }
            assertFalse(accepted, "Retained public keys cannot authorize live operations")
            assertTrue(rpc.destroySession(original.copy(proof = proof(SessionSigning.DESTROY, original.toByteArray()))).result.isOk())
            assertFalse(store.createIfAbsent(ServerSession(storedId, byteArrayOf(54), key.publicKey.encode())))
            val detached = requireNotNull(store.revokedVerificationKey(storedId))
            detached[0] = 0
            assertContentEquals(key.publicKey.encode(), store.revokedVerificationKey(storedId))
        } finally { component.stop(); db.close(); file.delete() }
    }

    @Test fun independentPostgresHandleCanConfirmCommittedDestruction(): Unit = runBlocking {
        val base = com.latenighthack.lockers.server.claim.PgTestGate.urlOrSkip()
        val schema = "destroy_retry_${System.nanoTime()}"
        java.sql.DriverManager.getConnection(base).use { it.createStatement().use { sql -> sql.execute("CREATE SCHEMA $schema") } }
        val location = base + (if (base.contains('?')) "&" else "?") + "currentSchema=$schema"
        val a = ServerStorage.postgres(location); val b = ServerStorage.postgres(location)
        try {
            a.open(); b.open()
            val first = com.latenighthack.lockers.server.services.session.v1.SessionStoreImpl(a)
            val second = com.latenighthack.lockers.server.services.session.v1.SessionStoreImpl(b)
            val key = Secp256r1KeyPair.generate(); val sid = SessionId(byteArrayOf(55)); val stored = ServerSessionId(sid.rawValue)
            first.updateSession(ServerSession(stored, byteArrayOf(56), key.publicKey.encode()))
            val unsigned = DestroySessionRequest(sessionId = sid).toByteArray()
            suspend fun destroy(database: Database, store: com.latenighthack.lockers.server.services.session.v1.SessionStore): Boolean {
                val now = System.currentTimeMillis(); val nonce = Random.nextBytes(32)
                val proof = SessionProof(now, nonce, Signature(signature = key.privateKey.sign(
                    SessionSigning.context(SessionSigning.DESTROY, sid, SHA256.digest(unsigned), now, nonce)), signingVersion = 2))
                return com.latenighthack.lockers.server.services.session.v1.SessionProofVerifier(database, store).authorize(
                    SessionSigning.DESTROY, sid, proof, unsigned, { false }) { store.destroySession(stored); true }
            }
            assertTrue(destroy(a, first))
            assertNull(second.getSessionById(stored))
            assertTrue(destroy(b, second), "A fresh process must confirm the committed terminal outcome")
            assertFalse(second.createIfAbsent(ServerSession(stored, byteArrayOf(57), key.publicKey.encode())))
        } finally {
            a.close(); b.close()
            java.sql.DriverManager.getConnection(base).use { it.createStatement().use { sql -> sql.execute("DROP SCHEMA $schema CASCADE") } }
        }
    }

}
