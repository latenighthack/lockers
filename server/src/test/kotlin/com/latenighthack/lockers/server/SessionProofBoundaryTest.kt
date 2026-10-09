package com.latenighthack.lockers.server

import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.SessionSigning
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.SessionGatewayDiscovery
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.session.v1.SessionGatewayService
import kotlinx.coroutines.runBlocking
import kotlin.random.Random
import kotlin.test.*

class SessionProofBoundaryTest {
    @Test fun `public subscriptions require possession and reject replay or parameter substitution`() = runBlocking {
        val db = ServerStorage.inMemory()
        val core = ServerCore::class.create(LockersConfig.defaults().copy(deliveryWorkerEnabled = false), db)
        core.setup()
        val key = Secp256r1KeyPair.generate()
        val session = SessionId(byteArrayOf(1))
        core.sessionStore.updateSession(ServerSession(sessionId = ServerSessionId(session.rawValue), authorizedPublicKey = key.publicKey.encode(), nextKeyMaterial = byteArrayOf(2)))
        val module = RoomServiceModule::class.create(core, object : SessionGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = null
        }, LocalRoomOwnership())
        val rpc = LocalRoomServiceRpc(module.server)
        val request = SubscriptionRequest { sessionId = session; roomId = RoomId(byteArrayOf(3)); kind.subscribe {} }
        try {
            assertFalse(rpc.subscription(request).result.isOk())
            val issued = System.currentTimeMillis(); val nonce = Random.nextBytes(32)
            val proof = SessionProof(issued, nonce, Signature(signature = key.privateKey.sign(
                SessionSigning.context(SessionSigning.SUBSCRIPTION, session, SHA256.digest(request.toByteArray()), issued, nonce)), signingVersion = 2))
            assertFalse(rpc.subscription(request.copy(roomId = RoomId(byteArrayOf(4)), proof = proof)).result.isOk())
            assertTrue(rpc.subscription(request.copy(proof = proof)).result.isOk())
            assertFalse(rpc.subscription(request.copy(proof = proof)).result.isOk())
            assertEquals(listOf(ServerRoomId(byteArrayOf(3))), core.subscriptionStore.getAllSubscriptions(ServerSessionId(session.rawValue)))
        } finally { module.serverImpl.close(); db.close() }
    }
}
