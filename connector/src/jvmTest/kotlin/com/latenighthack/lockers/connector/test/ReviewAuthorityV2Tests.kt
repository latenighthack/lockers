package com.latenighthack.lockers.connector.test

import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.LockerSigning
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.room.v1.*
import kotlinx.coroutines.*
import kotlin.test.*

class ReviewAuthorityV2Tests {
    @Test fun `V2 signed ratchet write binds authority incarnation and final notification`() = runBlocking {
        val key = Secp256r1KeyPair.generate()
        val room = RoomId(byteArrayOf(1)); val id = LockerId(byteArrayOf(2), LockerKeyspace(3))
        var request: PostLockerChangeRequest? = null
        var activeKey = key
        val client = reviewClient(ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(authorityV2 = true, writeReceipts = true).toByteArray()
            "GetLocker" -> GetLockerResponse(locker = IdentifiedLocker(id, lockState = LockState(locked = true, lockVersion = 5, publicKey = Secp256R1Key.PublicKey(key.publicKey.encode())))).toByteArray()
            "PostLockerChange" -> { request = PostLockerChangeRequest.fromByteArray(bytes); PostLockerChangeResponse(version = 1).toByteArray() }
            else -> error(method.methodName)
        } }, object : LockKeySource {
            override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = activeKey
            override suspend fun onRatcheted(roomId: RoomId, lockerId: LockerId, newKeyPair: Secp256r1KeyPair) { activeKey = newKeyPair }
        })
        try {
            client.updateLocker(room, id, { payload { rawValue = byteArrayOf(9) } }, ratchet = true) { byteArrayOf(4) }
            val written = request!!
            assertEquals(2, written.writeSignature!!.signingVersion)
            assertTrue(key.publicKey.verify(LockerSigning.writeContextV2(room, id, written.parentVersion, 5, SHA256.digest(byteArrayOf(4)), written.notification), written.writeSignature!!.signature))
            assertEquals(2, written.ratchet!!.signature!!.signingVersion)
            assertTrue(key.publicKey.verify(LockerSigning.ratchetContextV2(room, id, written.parentVersion, 5, written.ratchet!!.newPublicKey!!.rawValue, written.ratchet!!.newSharedKeys), written.ratchet!!.signature!!.signature))
        } finally { client.closeAndJoin() }
    }
    @Test fun `grant discovers scope and parent incarnations and unlock binds its expected version`() = runBlocking {
        val parent = Secp256r1KeyPair.generate(); val child = Secp256r1KeyPair.generate()
        val room = RoomId(byteArrayOf(1)); val scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_KEYSPACE, keyspace = LockerKeyspace(3))
        var grantRequest: LockLockerRequest? = null
        var unlockRequest: UnlockLockerRequest? = null
        val client = reviewClient(ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(authorityV2 = true).toByteArray()
            "GetLockScope" -> GetLockScopeResponse(scopeState = LockState(scope = scope, lockVersion = 3), parentState = LockState(locked = true, lockVersion = 7)).toByteArray()
            "LockLocker" -> { grantRequest = LockLockerRequest.fromByteArray(bytes); LockLockerResponse().toByteArray() }
            "UnlockLocker" -> { unlockRequest = UnlockLockerRequest.fromByteArray(bytes); UnlockLockerResponse().toByteArray() }
            else -> error(method.methodName)
        } })
        try {
            client.lockLocker(room, scope, child, parent)
            val request = grantRequest!!; val grant = request.grant!!
            assertEquals(3, request.parentLockVersion)
            assertEquals(7, grant.authorityVersion); assertEquals(3, grant.scopeVersion)
            assertEquals(2, grant.parentSignature!!.signingVersion)
            assertTrue(parent.publicKey.verify(LockerSigning.grantContextV2(room, scope, grant.publicKey!!.rawValue, 7, 3), grant.parentSignature!!.signature))
            client.unlockLocker(room, scope, child, 4)
            assertEquals(2, unlockRequest!!.signature!!.signingVersion)
            assertTrue(child.publicKey.verify(LockerSigning.unlockContextV2(room, scope, 4), unlockRequest!!.signature!!.signature))
        } finally { client.closeAndJoin() }
    }
    @Test fun `ambiguous response reuses its transformed and encoded request without recomputing`() = runBlocking {
        var transforms = 0; var encodes = 0; var attempts = 0
        var original: ByteArray? = null
        val codec = object : NotificationCodec {
            override suspend fun decode(context: NotificationContext, payload: ByteArray) = payload
            override suspend fun encode(context: NotificationContext, payload: ByteArray): ByteArray { encodes++; return payload }
        }
        val client = reviewClient(ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(writeReceipts = true).toByteArray()
            "PostLockerChange" -> {
                if (original == null) original = bytes else assertContentEquals(original, bytes)
                if (++attempts == 1) throw FaultInjectingRpcClient.rpcError(com.latenighthack.ktbuf.proto.Codes.UNAVAILABLE)
                PostLockerChangeResponse(version = 1).toByteArray()
            }
            else -> error(method.methodName)
        } }, codecs = NotificationCodecs.of(codec))
        try {
            client.updateLocker(RoomId(byteArrayOf(1)), LockerId(byteArrayOf(2)), { payload { rawValue = byteArrayOf(9) } }) { transforms++; byteArrayOf(4) }
            assertEquals(2, attempts); assertEquals(1, transforms); assertEquals(1, encodes)
        } finally { client.closeAndJoin() }
    }

    @Test fun `ambiguous retries freeze caller-owned payload arrays`() = runBlocking {
        val transformed = byteArrayOf(4); val notification = byteArrayOf(9)
        var attempts = 0; var original: ByteArray? = null
        val client = reviewClient(ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(writeReceipts = true).toByteArray()
            "PostLockerChange" -> {
                if (original == null) original = bytes.copyOf() else assertContentEquals(original, bytes)
                if (++attempts == 1) {
                    transformed.fill(8); notification.fill(7)
                    throw FaultInjectingRpcClient.rpcError(com.latenighthack.ktbuf.proto.Codes.UNAVAILABLE)
                }
                PostLockerChangeResponse(version = 1).toByteArray()
            }
            else -> error(method.methodName)
        } })
        try {
            client.updateLocker(RoomId(byteArrayOf(1)), LockerId(byteArrayOf(2)), { payload { rawValue = notification } }) { transformed }
            assertEquals(2, attempts)
        } finally { client.closeAndJoin() }
    }
}
