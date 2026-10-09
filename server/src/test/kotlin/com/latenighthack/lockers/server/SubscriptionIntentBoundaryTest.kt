package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.net.RpcResponseException
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.SessionSigning
import com.latenighthack.lockers.common.RoomKeying
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.SessionGatewayDiscovery
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.session.v1.SessionGatewayService
import com.latenighthack.lockers.server.agents.LockerAgentRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.runBlocking
import kotlin.random.Random
import kotlin.test.*

class SubscriptionIntentBoundaryTest {
    private class Fixture {
        val db = ServerStorage.inMemory()
        val core = ServerCore::class.create(LockersConfig.defaults().copy(deliveryWorkerEnabled = false), db)
        val room = RoomId(byteArrayOf(2)); val sid = SessionId(byteArrayOf(1))
        lateinit var key: Secp256r1KeyPair
        lateinit var module: RoomServiceModule
        lateinit var rpc: RoomService
        suspend fun open() {
            core.setup(); key = Secp256r1KeyPair.generate()
            core.sessionStore.updateSession(ServerSession(sessionId = ServerSessionId(sid.rawValue), authorizedPublicKey = key.publicKey.encode(), nextKeyMaterial = byteArrayOf(3)))
            module = RoomServiceModule::class.create(core, object : SessionGatewayDiscovery { override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = null }, LocalRoomOwnership())
            rpc = LocalRoomServiceRpc(module.server)
            for (index in 1..3) core.lockerStore.updateLocker(ServerLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(byteArrayOf(index.toByte())),
                Locker { open { encodedPayload = byteArrayOf(index.toByte()) } }.toByteArray(), 1))
        }
        suspend fun proof(operation: String, bytes: ByteArray): SessionProof {
            val issued = System.currentTimeMillis(); val nonce = Random.nextBytes(32)
            return SessionProof(issued, nonce, Signature(signingVersion = 2, signature = key.privateKey.sign(SessionSigning.context(operation, sid, SHA256.digest(bytes), issued, nonce))))
        }
        suspend fun subscription(revision: Long, subscribe: Boolean): SubscriptionResponse {
            val request = SubscriptionRequest { sessionId = sid; roomId = room; intentRevision = revision; if (subscribe) kind.subscribe {} else kind.unsubscribe {} }
            return rpc.subscription(request.copy(proof = proof(SessionSigning.SUBSCRIPTION, request.toByteArray())))
        }
        suspend fun snapshot(revision: Long, token: ByteArray = byteArrayOf(), size: Int = 1, spaces: List<LockerKeyspace> = emptyList()): SubscribeAndSnapshotResponse {
            val request = SubscribeAndSnapshotRequest(roomId = room, sessionId = sid, intentRevision = revision, pageSize = size, pageToken = token, keyspaces = spaces)
            return rpc.subscribeAndSnapshot(request.copy(proof = proof(SessionSigning.SNAPSHOT, request.toByteArray())))
        }
        suspend fun close() { module.serverImpl.closeAndJoin(); core.closeAndJoin(); db.close() }
    }
    @Test fun `signed snapshot pages observe their exact active generation and cannot restore revoked membership`() = runBlocking {
        val f = Fixture(); f.open()
        try {
            assertTrue(f.rpc.capabilities(CapabilitiesRequest()).subscriptionRevisions)
            val first = f.snapshot(1); assertTrue(first.result.isOk()); assertEquals(1L, first.currentRevision); assertEquals(36, first.nextPageToken.size)
            assertTrue(f.snapshot(1, first.nextPageToken).result.isOk())
            assertEquals(SubscriptionResponse.Result.STALE_INTENT, f.snapshot(0, first.nextPageToken).result)
            assertEquals(SubscriptionResponse.Result.STALE_INTENT, f.snapshot(2, first.nextPageToken).result)
            assertTrue(f.subscription(2, true).result.isOk())
            assertEquals(2L, f.snapshot(1, first.nextPageToken).currentRevision)
            assertEquals(Codes.INVALID_ARGUMENT, assertFailsWith<RpcResponseException> { f.snapshot(2, first.nextPageToken) }.code, "token binding must include generation")
            val fresh = f.snapshot(2)
            assertTrue(fresh.result.isOk()); assertTrue(f.snapshot(2, size = 2, spaces = listOf(LockerKeyspace(0))).result.isOk(), "same desired generation may use different page parameters")
            assertTrue(f.subscription(3, false).result.isOk())
            val stale = f.snapshot(2, fresh.nextPageToken); assertEquals(SubscriptionResponse.Result.STALE_INTENT, stale.result); assertEquals(3L, stale.currentRevision); assertTrue(stale.lockers.isEmpty())
            assertEquals(SubscriptionResponse.Result.STALE_INTENT, f.snapshot(2).result)
            assertTrue(f.core.subscriptionStore.getAllSubscriptions(ServerSessionId(f.sid.rawValue)).isEmpty())
            assertEquals(Codes.FAILED_PRECONDITION, assertFailsWith<RpcResponseException> { f.snapshot(3) }.code)
            assertTrue(f.snapshot(4).result.isOk())
            assertEquals(listOf(ServerRoomId(f.room.rawValue)), f.core.subscriptionStore.getAllSubscriptions(ServerSessionId(f.sid.rawValue)))
        } finally { f.close() }
    }
    @Test fun `permanent session destruction clears revision and snapshot ledgers and rejects delayed signed mutations`() = runBlocking {
        val f = Fixture(); f.open()
        try {
            assertTrue(f.snapshot(1).result.isOk()); assertTrue(f.subscription(2, false).result.isOk())
            val definition = SubscriptionIntentDefinitionV2
            assertEquals(1L, f.db.count(definition.storeName, definition.session.query(1, lower = f.sid.rawValue, upper = f.sid.rawValue)))
            f.core.sessionStore.destroySession(ServerSessionId(f.sid.rawValue))
            assertEquals(0L, f.db.count(definition.storeName, definition.session.query(1, lower = f.sid.rawValue, upper = f.sid.rawValue)))
            assertTrue(f.core.sessionStore.isRevoked(ServerSessionId(f.sid.rawValue)))
            assertEquals(SubscriptionResponse.Result.UNKNOWN_ERROR, f.subscription(3, true).result)
            assertEquals(SubscriptionResponse.Result.UNKNOWN_ERROR, f.snapshot(3).result)
            assertTrue(f.core.subscriptionStore.getAllSubscriptions(ServerSessionId(f.sid.rawValue)).isEmpty())
            assertEquals(0L, f.db.count(SnapshotDefinitionV2.storeName, SnapshotDefinitionV2.session.query(1, lower = f.sid.rawValue, upper = f.sid.rawValue)))
        } finally { f.close() }
    }
    @Test fun `signed malformed room identities never persist membership or intent authority`() = runBlocking {
        val f = Fixture(); f.open()
        try {
            for (raw in listOf(byteArrayOf(), ByteArray(129), RoomKeying.publicKeyed(ByteArray(33) { 0xff.toByte() }.also { it[0] = 2 }).rawValue)) {
                val unsigned = SubscriptionRequest { sessionId = f.sid; roomId = RoomId(raw); intentRevision = 1; kind.subscribe {} }
                assertEquals(Codes.INVALID_ARGUMENT, assertFailsWith<RpcResponseException> {
                    f.rpc.subscription(unsigned.copy(proof = f.proof(SessionSigning.SUBSCRIPTION, unsigned.toByteArray())))
                }.code)
            }
            assertTrue(f.core.subscriptionStore.getAllSubscriptions(ServerSessionId(f.sid.rawValue)).isEmpty())
            val definition = SubscriptionIntentDefinitionV2
            assertEquals(0L, f.db.count(definition.storeName, definition.session.query(1, lower = f.sid.rawValue, upper = f.sid.rawValue)))
        } finally { f.close() }
    }
    @Test fun `invalid stored snapshot fences older intents without confirming membership and repaired retry reuses the same revision`() = runBlocking {
        val f = Fixture(); f.open()
        try {
            f.core.lockerStore.updateLocker(ServerLocker(ServerRoomId(f.room.rawValue), 0, ServerLockerId(byteArrayOf(1)), byteArrayOf(0xff.toByte()), 1))
            val invalid = f.snapshot(2); assertEquals(SubscriptionResponse.Result.INVALID_DATA, invalid.result); assertEquals(2L, invalid.currentRevision); assertTrue(invalid.nextPageToken.isEmpty())
            assertTrue(f.core.subscriptionStore.getAllSubscriptions(ServerSessionId(f.sid.rawValue)).isEmpty())
            assertEquals(SubscriptionResponse.Result.STALE_INTENT, f.subscription(1, false).result)
            val absent = f.core.subscriptionStore.observeIntent(ServerSessionId(f.sid.rawValue), ServerRoomId(f.room.rawValue), 2) { error("desired fence does not confirm live membership") }
            assertTrue(absent.stale)
            f.core.lockerStore.updateLocker(ServerLocker(ServerRoomId(f.room.rawValue), 0, ServerLockerId(byteArrayOf(1)), Locker { open { encodedPayload = byteArrayOf(9) } }.toByteArray(), 1))
            val repaired = f.snapshot(2); assertTrue(repaired.result.isOk()); assertEquals(2L, repaired.currentRevision)
            assertEquals(listOf(ServerRoomId(f.room.rawValue)), f.core.subscriptionStore.getAllSubscriptions(ServerSessionId(f.sid.rawValue)))
        } finally { f.close() }
    }
    @Test fun `custom legacy subscription stores do not advertise or silently accept modern intent ordering`() = runBlocking {
        val db = ServerStorage.inMemory(); db.open(); var calls = 0
        val custom = object : SubscriptionStore {
            override suspend fun getAllSubscriptions(sessionId: ServerSessionId) = emptyList<ServerRoomId>()
            override suspend fun getAllSessions(roomId: ServerRoomId) = emptyList<ServerSessionId>()
            override suspend fun addSubscription(sessionId: ServerSessionId, roomId: ServerRoomId) { calls++ }
            override suspend fun removeSubscription(sessionId: ServerSessionId, roomId: ServerRoomId) { calls++ }
        }
        val service = RoomServiceImpl(custom, LockerStoreImpl(db), LockStoreImpl(db), object : SessionGatewayDiscovery { override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = null }, LocalRoomOwnership(), LockerAgentRegistry.None, SimpleMeterRegistry(), LockersConfig.defaults().copy(deliveryWorkerEnabled = false))
        val rpc = LocalRoomServiceRpc(service)
        fun request(revision: Long) = SubscriptionRequest { sessionId = SessionId(byteArrayOf(1)); roomId = RoomId(byteArrayOf(2)); intentRevision = revision; kind.subscribe {} }
        try {
            assertFalse(rpc.capabilities(CapabilitiesRequest()).subscriptionRevisions)
            assertEquals(Codes.UNIMPLEMENTED, assertFailsWith<RpcResponseException> { rpc.subscription(request(1)) }.code)
            assertEquals(Codes.UNIMPLEMENTED, assertFailsWith<RpcResponseException> { rpc.subscribeAndSnapshot(SubscribeAndSnapshotRequest(roomId = RoomId(byteArrayOf(2)), sessionId = SessionId(byteArrayOf(1)), intentRevision = 1)) }.code)
            assertEquals(0, calls); assertTrue(rpc.subscription(request(0)).result.isOk()); assertEquals(1, calls)
        } finally { service.closeAndJoin(); db.close() }
    }
}
