package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktbuf.test.server.runTestWithServer
import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.InMemoryKeyValueStoreDelegate
import com.latenighthack.ktstore.InMemoryStoreDelegate
import com.latenighthack.ktstore.KeyValueStore
import com.latenighthack.lockers.common.v1.LockerId
import com.latenighthack.lockers.common.v1.LockerKeyspace
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.common.v1.Version
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.example.v1.*
import com.latenighthack.lockers.server.*
import com.latenighthack.lockers.server.services.push.v1.providers.PushBackendKind
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals

class PushRegistrationTests {

    private val KEYSPACE = LockerKeyspace { value = 1L }

    private fun randomRoomId() = RoomId(Random.nextBytes(32))
    private fun randomLockerId(keyspace: LockerKeyspace) = LockerId(Random.nextBytes(32), keyspace)

    private suspend fun createClient(rpcClient: RpcClient): LockersClient {
        val keyPair = Secp256r1KeyPair.generate()
        val keySource = object : AuthenticationKeySource {
            override suspend fun getSessionKeyPair() = keyPair
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
        ).also { it.awaitConnected() }
    }

    @Test(timeout = 15_000)
    fun `a registered device receives a push for a subscribed update`() {
        val apnsProvider = RecordingPushProvider(PushBackendKind.APNS)
        runTestWithServer({ attachTestServicesWith { it.overridePushProviders = listOf(apnsProvider) } }) { server, _ ->
            val client = createClient(server.rpcClient)
            val typed = client.typed(KEYSPACE, ExampleLocker::toByteArray, ExampleLocker.Companion::fromByteArray)
            val roomId = randomRoomId()
            val lockerId = randomLockerId(KEYSPACE)

            client.registerPush(PushRegistrations.apns("device-token".encodeToByteArray()))
            client.awaitPushRegistered(PushBackendType.APNS)

            typed.subscribeToRoom(roomId)
            typed.updateLocker(roomId, lockerId, {
                push { title = "hi"; body = "yo" }
                payload { rawValue = "p".encodeToByteArray() }
            }) { it.copy { } }

            // No withTimeout here: runTestWithServer uses a virtual clock, so a
            // virtual-time timeout would fire before the real push arrives. The
            // @Test(timeout) provides the wall-clock guard instead.
            apnsProvider.awaitSends(1)
            assertEquals("hi", apnsProvider.sends.first().title)
            client.close()
        }
    }
}
