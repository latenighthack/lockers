package com.latenighthack.lockers.connector

import com.latenighthack.ktbuf.net.*
import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.InMemoryKeyValueStoreDelegate
import com.latenighthack.ktstore.KeyValueStore
import com.latenighthack.lockers.common.LockerSigning
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.internal.LockerStoreImpl
import com.latenighthack.lockers.room.v1.*
import kotlinx.coroutines.test.runTest
import kotlin.test.*

class AuthorityRepairPortabilityTest {
    private var debugLogging = false
    @BeforeTest fun avoidPlatformDebugLogging() {
        // Pure protocol tests also run on the Android JVM, where android.util.Log is a stub.
        debugLogging = com.diamondedge.logging.KmLogging.isLoggingDebug
        com.diamondedge.logging.KmLogging.isLoggingDebug = false
    }
    @AfterTest fun restoreDebugLogging() {
        com.diamondedge.logging.KmLogging.isLoggingDebug = debugLogging
    }
    @Test fun authorityOnlyConflictPreservesFrozenContentOnEveryTarget() = exercise(false)
    @Test fun unlockedAuthorityHistoryCanBeRelockedWithTheOwnedKey() = exercise(true)
    private fun exercise(unlockedHistory: Boolean) = runTest {
        val key = Secp256r1KeyPair.generate()
        val room = RoomId(byteArrayOf(1))
        val id = LockerId(byteArrayOf(2), LockerKeyspace(3))
        val requests = mutableListOf<PostLockerChangeRequest>()
        val state = LockState(locked = true, scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM),
            lockVersion = if (unlockedHistory) 3 else 1, publicKey = Secp256R1Key.PublicKey(key.publicKey.encode()))
        val observed = if (unlockedHistory) state.copy(locked = false, lockVersion = 2, publicKey = null) else null
        val rpc = object : RpcClient {
            override suspend fun unaryCall(method: RpcMethodSpecifier, headers: Map<String, String>, request: ByteArray): RpcResponse {
                val bytes = when (method.methodName) {
                    "Capabilities" -> CapabilitiesResponse(authorityV2 = true, writeReceipts = true).toByteArray()
                    "GetLocker" -> GetLockerResponse(locker = IdentifiedLocker(id, lockState = observed)).toByteArray()
                    "PostLockerChange" -> {
                        val posted = PostLockerChangeRequest.fromByteArray(request)
                        requests += posted
                        if (requests.size == 1) PostLockerChangeResponse(
                            result = PostLockerChangeResponse.Result.SIGNATURE_INVALID, lockState = state).toByteArray()
                        else {
                            val signature = requireNotNull(posted.writeSignature)
                            assertTrue(key.publicKey.verify(LockerSigning.writeContextV2(room, id, 0, state.lockVersion,
                                SHA256.digest(byteArrayOf(4)), posted.notification), signature.signature))
                            assertContentEquals(signature.toByteArray(),
                                posted.locker!!.sealed!!.payload!!.enclosure!!.signature!!.toByteArray())
                            PostLockerChangeResponse(version = 1, lockState = state).toByteArray()
                        }
                    }
                    else -> error(method.methodName)
                }
                return RpcResponse(bytes, emptyMap())
            }
            override suspend fun serverStreamingCall(method: RpcMethodSpecifier,
                block: suspend RpcServerStream.() -> Unit, readyCallback: () -> Unit) = error("Not used")
        }
        val database = ConnectorStorage.inMemory()
        database.open()
        val authentication = object : AuthenticationKeySource {
            override suspend fun getSessionKeyPair() = key
            override suspend fun hasSessionKeyPair() = true
            override suspend fun generateSessionKeyPair() = Unit
            override suspend fun revokeKeys() = Unit
        }
        var transforms = 0
        var encodes = 0
        val codec = object : NotificationCodec {
            override suspend fun decode(context: NotificationContext, payload: ByteArray) = payload
            override suspend fun encode(context: NotificationContext, payload: ByteArray): ByteArray { encodes++; return payload }
        }
        val session = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), database)
        val client = LockerClient(rpc, Stream(rpc, authentication, session, SubscriptionStoreImpl(database), Version()),
            LockerStoreImpl(database), object : LockKeySource {
                override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId) = key
            }, NotificationCodecs.of(codec))
        try {
            client.updateLocker(room, id, { payload { rawValue = byteArrayOf(5) } }) { transforms++; byteArrayOf(4) }
            assertEquals(2, requests.size)
            assertEquals(1, transforms)
            assertEquals(1, encodes)
            val original = requests[0]
            val repaired = requests[1]
            assertFalse(original.writeRequestId.contentEquals(repaired.writeRequestId))
            assertContentEquals(original.notification!!.toByteArray(), repaired.notification!!.toByteArray())
            assertContentEquals(original.locker!!.sealed!!.payload!!.enclosure!!.innerPayload,
                repaired.locker!!.sealed!!.payload!!.enclosure!!.innerPayload)
        } finally { client.closeAndJoin(); database.close() }
    }
}
