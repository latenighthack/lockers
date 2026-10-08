package com.latenighthack.lockers.server.services.session.v1

import com.latenighthack.ktbuf.net.GrpcRequestContext
import com.latenighthack.lockers.common.SessionSigning
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.push.v1.*
import com.latenighthack.lockers.session.v1.*

class AuthorizedRoomServer(private val trusted: RoomServer, private val proofs: SessionProofVerifier) : RoomServer by trusted {
    override suspend fun subscription(context: GrpcRequestContext, request: SubscriptionRequest) =
        proofs.authorize(SessionSigning.SUBSCRIPTION, request.sessionId, request.proof, request.copy(proof = null).toByteArray(),
            { SubscriptionResponse(SubscriptionResponse.Result.UNKNOWN_ERROR) }) { trusted.subscription(context, request) }
    override suspend fun subscribeAndSnapshot(context: GrpcRequestContext, request: SubscribeAndSnapshotRequest) =
        proofs.authorize(SessionSigning.SNAPSHOT, request.sessionId, request.proof, request.copy(proof = null).toByteArray(),
            { SubscribeAndSnapshotResponse(SubscriptionResponse.Result.UNKNOWN_ERROR) }) { trusted.subscribeAndSnapshot(context, request) }
}

class AuthorizedPushServer(private val trusted: PushServer, private val proofs: SessionProofVerifier) : PushServer by trusted {
    override suspend fun registerSession(context: GrpcRequestContext, request: RegisterSessionRequest) =
        proofs.authorize(SessionSigning.REGISTER_PUSH, request.sessionId, request.proof, request.copy(proof = null).toByteArray(),
            { RegisterSessionResponse(RegisterSessionResponse.Result.UNKNOWN_ERROR) }) { trusted.registerSession(context, request) }
    override suspend fun unregisterSession(context: GrpcRequestContext, request: UnregisterSessionRequest) =
        proofs.authorize(SessionSigning.UNREGISTER_PUSH, request.sessionId, request.proof, request.copy(proof = null).toByteArray(),
            { UnregisterSessionResponse(UnregisterSessionResponse.Result.UNKNOWN_ERROR) }) { trusted.unregisterSession(context, request) }
}

class AuthorizedSessionServer(private val trusted: SessionServer, private val proofs: SessionProofVerifier) : SessionServer by trusted {
    override suspend fun destroySession(context: GrpcRequestContext, request: DestroySessionRequest) =
        proofs.authorize(SessionSigning.DESTROY, request.sessionId, request.proof, request.copy(proof = null).toByteArray(),
            { DestroySessionResponse(DestroySessionResponse.Result.INVALID_SEQUENCE) }) { trusted.destroySession(context, request) }
}
