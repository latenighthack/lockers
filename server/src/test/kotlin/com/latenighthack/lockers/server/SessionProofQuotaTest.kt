package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.net.RpcResponseException
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.SessionSigning
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import kotlinx.coroutines.runBlocking
import kotlin.test.*

class SessionProofQuotaTest {
    @Test fun validFreshProofsAreBoundedWithoutPruningReplayProtection() = runBlocking {
        val db = ServerStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(db); val key = Secp256r1KeyPair.generate()
        val a = SessionId(byteArrayOf(1)); val b = SessionId(byteArrayOf(2)); val c = SessionId(byteArrayOf(3))
        var now = 1_000_000L
        val limits = ServerResourceLimits(maxOutstandingProofs = 2, maxOutstandingProofsPerSession = 1)
        val verifier = SessionProofVerifier(db, sessions, { now }, limits)
        val request = byteArrayOf(4); var mutations = 0
        suspend fun authorize(sid: SessionId, nonceByte: Byte): Boolean {
            val nonce = ByteArray(32) { nonceByte }
            val context = SessionSigning.context(SessionSigning.SUBSCRIPTION, sid, SHA256.digest(request), now, nonce)
            val proof = SessionProof(issuedAtMs = now, nonce = nonce, signature = com.latenighthack.lockers.common.v1.Signature(signature = key.privateKey.sign(context), signingVersion = 2))
            return verifier.authorize(SessionSigning.SUBSCRIPTION, sid, proof, request, { false }) { mutations++; true }
        }
        try {
            for (sid in listOf(a, b, c)) sessions.updateSession(ServerSession(ServerSessionId(sid.rawValue), ByteArray(32), key.publicKey.encode()))
            assertTrue(authorize(a, 1))
            val sameSession = assertFailsWith<RpcResponseException> { authorize(a, 2) }
            assertEquals(Codes.RESOURCE_EXHAUSTED, sameSession.code)
            assertFalse(authorize(a, 1))
            assertTrue(authorize(b, 3))
            assertEquals(Codes.RESOURCE_EXHAUSTED, assertFailsWith<RpcResponseException> { authorize(b, 4) }.code)
            assertEquals(Codes.RESOURCE_EXHAUSTED, assertFailsWith<RpcResponseException> { authorize(c, 6) }.code)
            assertEquals(2, mutations)
            now += SessionProofVerifier.WINDOW_MS + SessionProofVerifier.FUTURE_SKEW_MS + 1
            assertTrue(authorize(a, 5))
            assertEquals(3, mutations)
        } finally { db.close() }
    }
}
