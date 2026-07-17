package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktbuf.test.server.runTestWithServer
import com.latenighthack.ktcrypto.Secp256r1KeyPair
import com.latenighthack.ktcrypto.generate
import com.latenighthack.ktstore.InMemoryKeyValueStoreDelegate
import com.latenighthack.ktstore.InMemoryStoreDelegate
import com.latenighthack.ktstore.KeyValueStore
import com.latenighthack.lockers.common.v1.LockScope
import com.latenighthack.lockers.common.v1.LockScopeKind
import com.latenighthack.lockers.common.v1.LockerId
import com.latenighthack.lockers.common.v1.LockerKeyspace
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.common.v1.Version
import com.latenighthack.lockers.connector.AuthenticationKeySource
import com.latenighthack.lockers.connector.LockKeySource
import com.latenighthack.lockers.connector.LockersClient
import com.latenighthack.lockers.server.attachTestServices
import com.latenighthack.lockers.server.rpcClient
import io.ktor.server.application.Application
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.test.assertContentEquals

/**
 * Locker writes must survive multi-kilobyte payloads (widget gen-state is tens of KB): pins the
 * >8KB write failure seen live, bisected by layer — open write, signed write, signed+notification.
 */
class LargePayloadTests {

    private val KEYSPACE = LockerKeyspace { value = 17L }

    private class FixedKeySource(private val keyPair: Secp256r1KeyPair?) : LockKeySource {
        override suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId): Secp256r1KeyPair? = keyPair
    }

    private suspend fun client(rpcClient: RpcClient, lockKeySource: LockKeySource?): LockersClient {
        val sessionKeyPair = Secp256r1KeyPair.generate()
        val keySource = object : AuthenticationKeySource {
            override suspend fun getSessionKeyPair() = sessionKeyPair
            override suspend fun hasSessionKeyPair() = true
            override suspend fun generateSessionKeyPair() {}
            override suspend fun revokeKeys() {}
        }
        return LockersClient.create(
            rpcClient = rpcClient,
            storeDelegate = InMemoryStoreDelegate(),
            keyValueStore = KeyValueStore(InMemoryKeyValueStoreDelegate()),
            keySource = keySource,
            appVersion = Version(0, 0, 1),
            lockKeySource = lockKeySource,
        ).also { it.awaitConnected() }
    }

    @Test(timeout = 60_000)
    fun `open 16KB write round-trips`() =
        runTestWithServer(Application::attachTestServices) { server, _ ->
            val lockers = client(server.rpcClient, lockKeySource = null)
            val roomId = RoomId(Random.nextBytes(32))
            val lockerId = LockerId(Random.nextBytes(32), KEYSPACE)
            val payload = Random.nextBytes(16_000)

            lockers.lockers.updateLocker(roomId, lockerId) { payload }
            val read = lockers.lockers.getLocker(roomId, lockerId, revalidate = true)
            assertContentEquals(payload, read?.locker?.let { l -> l.open?.encodedPayload ?: l.sealed?.payload?.enclosure?.innerPayload })
            lockers.close()
        }

    @Test(timeout = 60_000)
    fun `signed 16KB write round-trips`() =
        runTestWithServer(Application::attachTestServices) { server, _ ->
            val roomKey = Secp256r1KeyPair.generate()
            val lockers = client(server.rpcClient, FixedKeySource(roomKey))
            val roomId = RoomId(Random.nextBytes(32))
            val lockerId = LockerId(Random.nextBytes(32), KEYSPACE)

            // Establish a locker-scope lock so writes must verify against roomKey.
            lockers.lockers.lockLocker(
                roomId,
                LockScope(kind = LockScopeKind.LOCK_SCOPE_LOCKER, keyspace = KEYSPACE, lockerRawValue = lockerId.rawValue),
                roomKey,
                parentKeyPair = null,
            )

            val payload = Random.nextBytes(16_000)
            lockers.lockers.updateLocker(roomId, lockerId) { payload }
            val read = lockers.lockers.getLocker(roomId, lockerId, revalidate = true)
            assertContentEquals(payload, read?.locker?.let { l -> l.open?.encodedPayload ?: l.sealed?.payload?.enclosure?.innerPayload })
            lockers.close()
        }

    @Test(timeout = 60_000)
    fun `signed 16KB write with notification round-trips`() =
        runTestWithServer(Application::attachTestServices) { server, _ ->
            val roomKey = Secp256r1KeyPair.generate()
            val lockers = client(server.rpcClient, FixedKeySource(roomKey))
            val roomId = RoomId(Random.nextBytes(32))
            val lockerId = LockerId(Random.nextBytes(32), KEYSPACE)
            lockers.lockers.subscribeToRoom(roomId)
            lockers.lockers.lockLocker(
                roomId,
                LockScope(kind = LockScopeKind.LOCK_SCOPE_LOCKER, keyspace = KEYSPACE, lockerRawValue = lockerId.rawValue),
                roomKey,
                parentKeyPair = null,
            )

            val payload = Random.nextBytes(16_000)
            lockers.lockers.updateLocker(
                roomId,
                lockerId,
                notificationBuilder = { payload { rawValue = ByteArray(64) } },
            ) { payload }
            val read = lockers.lockers.getLocker(roomId, lockerId, revalidate = true)
            assertTrue(read != null)
            lockers.close()
        }
}
