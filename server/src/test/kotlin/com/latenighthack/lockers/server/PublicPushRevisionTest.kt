package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.net.GrpcRequestContext
import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.SessionSigning
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.push.v1.*
import com.latenighthack.lockers.server.services.session.v1.AuthorizedPushServer
import com.latenighthack.lockers.server.storage.v1.*
import kotlinx.coroutines.runBlocking
import kotlin.random.Random
import kotlin.test.*

class PublicPushRevisionTest {
    @Test fun publicCredentialMutationsRequirePositiveSignedRevisions(): Unit = runBlocking {
        val db = ServerStorage.inMemory()
        val core = ServerCore::class.create(LockersConfig.defaults(), db)
        core.setup()
        val key = Secp256r1KeyPair.generate()
        val sid = SessionId(byteArrayOf(31))
        core.sessionStore.updateSession(ServerSession(ServerSessionId(sid.rawValue), byteArrayOf(32), key.publicKey.encode()))
        var mutations = 0
        val trusted = object : PushServer {
            override suspend fun registerSession(context: GrpcRequestContext, request: RegisterSessionRequest) =
                RegisterSessionResponse(RegisterSessionResponse.Result.OK).also { mutations++ }
            override suspend fun unregisterSession(context: GrpcRequestContext, request: UnregisterSessionRequest) =
                UnregisterSessionResponse(UnregisterSessionResponse.Result.OK).also { mutations++ }
            override suspend fun getPushConfig(context: GrpcRequestContext, request: GetPushConfigRequest) = GetPushConfigResponse()
        }
        val rpc = LocalPushServiceRpc(AuthorizedPushServer(trusted, core.sessionProofVerifier))
        suspend fun signed(operation: String, encoded: ByteArray): SessionProof {
            val now = System.currentTimeMillis(); val nonce = Random.nextBytes(32)
            return SessionProof(now, nonce, Signature(signature = key.privateKey.sign(
                SessionSigning.context(operation, sid, SHA256.digest(encoded), now, nonce)), signingVersion = 2))
        }
        try {
            for (revision in listOf(-1L, 0L, 1L)) {
                val register = RegisterSessionRequest(sessionId = sid, credentialRevision = revision)
                val unregister = UnregisterSessionRequest(sessionId = sid, credentialRevision = revision)
                assertEquals(revision > 0, rpc.registerSession(register.copy(proof = signed(SessionSigning.REGISTER_PUSH, register.toByteArray()))).result.isOk())
                assertEquals(revision > 0, rpc.unregisterSession(unregister.copy(proof = signed(SessionSigning.UNREGISTER_PUSH, unregister.toByteArray()))).result.isOk())
            }
            assertEquals(2, mutations)
        } finally { db.close() }
    }
}
