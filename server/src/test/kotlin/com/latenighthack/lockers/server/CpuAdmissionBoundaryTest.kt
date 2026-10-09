package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.SessionSigning
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.server.services.push.v1.PushGatewayDiscovery
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.push.v1.PushGatewayService
import com.latenighthack.lockers.session.v1.*
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlin.test.*

class CpuAdmissionBoundaryTest {
    private val context = GrpcRequestContext("", emptyMap(), emptyMap(), emptyMap(), SessionServer.Descriptor, SessionServer.Descriptor.methods[0])
    private fun limits(burst: Int) = ServerResourceLimits.fromEnv { name -> when (name) {
        "LOCKERS_CPU_UNITS_PER_SEC" -> "1"; "LOCKERS_CPU_BURST" -> burst.toString(); else -> null
    } }
    private val discovery = object : PushGatewayDiscovery { override suspend fun findServer(sessionId: SessionId): PushGatewayService? = null }
    private suspend fun first(service: SessionServiceImpl, request: WatchSessionRequest) = withTimeout(5000) {
        (service.watchSession(context, flow { emit(request); awaitCancellation() }).first { it is StreamControlEvent.Message }
            as StreamControlEvent.Message).message.response!!.getOpen()!!
    }
    @Test fun validLengthForgedProofsConsumeBudgetWithoutMutatingOrStagingReplayRows() = runBlocking {
        val db = ServerStorage.inMemory(); db.open(); val sessions = SessionStoreImpl(db)
        val sid = SessionId(byteArrayOf(1)); val key = Secp256r1KeyPair.generate()
        sessions.updateSession(ServerSession(ServerSessionId(sid.rawValue), authorizedPublicKey = key.publicKey.encode()))
        val verifier = SessionProofVerifier(db, sessions, limits = limits(12)); var mutations = 0
        try {
            suspend fun attempt(n: Int) = verifier.authorize(SessionSigning.SUBSCRIPTION, sid,
                SessionProof(System.currentTimeMillis(), ByteArray(32) { n.toByte() }, Signature(signature = ByteArray(64), signingVersion = 2)),
                byteArrayOf(1), { false }) { mutations++; true }
            assertFalse(verifier.authorize(SessionSigning.SUBSCRIPTION, sid,
                SessionProof(System.currentTimeMillis(), ByteArray(32), Signature(signature = ByteArray(32), signingVersion = 2)),
                byteArrayOf(1), { false }) { mutations++; true })
            repeat(3) { assertFalse(attempt(it)) }
            assertEquals(Codes.RESOURCE_EXHAUSTED, assertFailsWith<RpcResponseException> { attempt(4) }.code)
            assertEquals(0, mutations)
            assertEquals(0, db.count(UsedSessionProofDefinitionV2.storeName, UsedSessionProofDefinitionV2.identity.query(1)))
        } finally { db.close() }
    }
    @Test fun encodedProofBytesAreChargedBeforeHashingAndVerification() = runBlocking {
        val db = ServerStorage.inMemory(); db.open(); val sessions = SessionStoreImpl(db)
        val verifier = SessionProofVerifier(db, sessions, limits = limits(12))
        try {
            assertEquals(Codes.RESOURCE_EXHAUSTED, assertFailsWith<RpcResponseException> {
                verifier.authorize(SessionSigning.SUBSCRIPTION, SessionId(byteArrayOf(1)),
                    SessionProof(System.currentTimeMillis(), ByteArray(32), Signature(signature = ByteArray(64), signingVersion = 2)),
                    ByteArray(1024 * 1024), { false }) { error("No mutation without admission") }
            }.code)
        } finally { db.close() }
    }
    @Test fun malformedCheapShapesDoNotConsumeHandshakeBudgetAndInvalidCurveWorkIsBounded() = runBlocking {
        val db = ServerStorage.inMemory(); db.open(); val sessions = SessionStoreImpl(db)
        val service = SessionServiceImpl(sessions, SessionInboxStoreImpl(db), SimpleMeterRegistry(), discovery,
            LocalSessionOwnership(), LockersConfig.defaults().copy(resourceLimits = limits(2)))
        try {
            val key = Secp256r1KeyPair.generate().publicKey.encode()
            assertEquals(WatchSessionResponse.Open.Result.INVALID_SESSION_ID, first(service, WatchSessionRequest {
                request.create { sessionId = SessionId(byteArrayOf()); publicKey { rawValue = key } }
            }).result)
            val invalidCurve = byteArrayOf(2) + ByteArray(32) { -1 }
            assertEquals(WatchSessionResponse.Open.Result.INVALID_PUBLIC_KEY, first(service, WatchSessionRequest {
                request.create { sessionId = SessionId(byteArrayOf(1)); publicKey { rawValue = invalidCurve } }
            }).result)
            assertEquals(WatchSessionResponse.Open.Result.RESOURCE_EXHAUSTED, first(service, WatchSessionRequest {
                request.create { sessionId = SessionId(byteArrayOf(2)); publicKey { rawValue = invalidCurve } }
            }).result)
            assertTrue(sessions.getAllSessions().isEmpty())
        } finally { service.close(); db.close() }
    }
    @Test fun dependencyGraphSharesOneBudgetAcrossHandshakeProofAndWrites() = runBlocking {
        val db = ServerStorage.inMemory(); val core = ServerCore::class.create(LockersConfig.defaults().copy(
            resourceLimits = limits(16), deliveryWorkerEnabled = false), db); core.setup()
        val sessionModule = SessionServiceModule::class.create(core, discovery, LocalSessionOwnership(), SessionRegistry.Noop)
        val roomModule = RoomServiceModule::class.create(core, object : SessionGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = null
        }, LocalRoomOwnership())
        val sid = SessionId(byteArrayOf(1)); val key = Secp256r1KeyPair.generate()
        try {
            val public = key.publicKey.encode()
            assertTrue(first(sessionModule.serverImpl, WatchSessionRequest { request.create { sessionId = sid; publicKey { rawValue = public } } }).result.isOk())
            val encoded = byteArrayOf(1); val issued = System.currentTimeMillis(); val nonce = ByteArray(32) { 1 }
            val signature = key.privateKey.sign(SessionSigning.context(SessionSigning.SUBSCRIPTION, sid, SHA256.digest(encoded), issued, nonce))
            assertTrue(core.sessionProofVerifier.authorize(SessionSigning.SUBSCRIPTION, sid, SessionProof(issued, nonce,
                Signature(signature = signature, signingVersion = 2)), encoded, { false }) { true })
            val room = RoomId(byteArrayOf(2)); val id = LockerId(byteArrayOf(3))
            assertEquals(Codes.RESOURCE_EXHAUSTED, assertFailsWith<RpcResponseException> {
                LocalRoomServiceRpc(roomModule.serverImpl).postLockerChanges(PostLockerChangesRequest(roomId = room, writeRequestId = ByteArray(16) { 4 },
                    changes = listOf(PostLockerChangeRequest(roomId = room, lockerId = id, locker = Locker { open { encodedPayload = byteArrayOf(1) } }))))
            }.code)
            assertNull(core.lockerStore.getLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(id.rawValue)))
        } finally { roomModule.serverImpl.close(); sessionModule.serverImpl.close(); db.close() }
    }
}
