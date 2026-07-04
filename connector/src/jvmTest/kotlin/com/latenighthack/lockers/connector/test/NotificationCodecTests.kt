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
import io.ktor.server.application.Application
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.launch
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals

class NotificationCodecTests {

    private val KEYSPACE = LockerKeyspace { value = 1L }
    private val OTHER_KEYSPACE = LockerKeyspace { value = 30L }

    private fun randomRoomId() = RoomId(Random.nextBytes(32))
    private fun randomLockerId(keyspace: LockerKeyspace) = LockerId(Random.nextBytes(32), keyspace)

    private suspend fun createClient(rpcClient: RpcClient, codecs: NotificationCodecs): LockersClient {
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
            codecs = codecs,
        ).also { it.awaitConnected() }
    }

    private fun xorCodec(both: Boolean) = object : NotificationCodec {
        private fun flip(bytes: ByteArray) = ByteArray(bytes.size) { (bytes[it].toInt() xor 0xFF).toByte() }
        override suspend fun decode(context: NotificationContext, payload: ByteArray) = flip(payload)
        override suspend fun encode(context: NotificationContext, payload: ByteArray) =
            if (both) flip(payload) else payload
    }

    private fun appendCodec(marker: Byte) = object : NotificationCodec {
        override suspend fun encode(context: NotificationContext, payload: ByteArray) = payload + marker
        override suspend fun decode(context: NotificationContext, payload: ByteArray) =
            if (payload.isNotEmpty() && payload.last() == marker) payload.copyOf(payload.size - 1) else payload
    }

    @Test(timeout = 15_000)
    fun `symmetric codec round-trips encode then decode`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val client = createClient(server.rpcClient, NotificationCodecs.of(xorCodec(both = true)))
        val typed = client.typed(KEYSPACE, ExampleLocker::toByteArray, ExampleLocker.Companion::fromByteArray)
        val roomId = randomRoomId()
        val lockerId = randomLockerId(KEYSPACE)

        val ready = CompletableDeferred<Unit>()
        val received = CompletableDeferred<ByteArray>()
        val job = launch {
            client.lockers.notifications.onStart { ready.complete(Unit) }.first().let { received.complete(it.payload) }
        }

        ready.await()
        typed.subscribeToRoom(roomId)
        typed.updateLocker(roomId, lockerId, { payload { rawValue = "hello".encodeToByteArray() } }) { it.copy { } }

        assertEquals("hello", received.await().decodeToString())
        job.cancel()
        client.close()
    }

    @Test(timeout = 15_000)
    fun `codecs compose and decode reverses their order`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val client = createClient(server.rpcClient, NotificationCodecs.of(appendCodec(0x01), appendCodec(0x02)))
        val typed = client.typed(KEYSPACE, ExampleLocker::toByteArray, ExampleLocker.Companion::fromByteArray)
        val roomId = randomRoomId()
        val lockerId = randomLockerId(KEYSPACE)

        val ready = CompletableDeferred<Unit>()
        val received = CompletableDeferred<ByteArray>()
        val job = launch {
            client.lockers.notifications.onStart { ready.complete(Unit) }.first().let { received.complete(it.payload) }
        }

        ready.await()
        typed.subscribeToRoom(roomId)
        typed.updateLocker(roomId, lockerId, { payload { rawValue = "data".encodeToByteArray() } }) { it.copy { } }

        assertEquals("data", received.await().decodeToString())
        job.cancel()
        client.close()
    }

    @Test(timeout = 15_000)
    fun `a codec returning null drops the notification`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val dropZeroPrefixed = object : NotificationCodec {
            override suspend fun decode(context: NotificationContext, payload: ByteArray): ByteArray? =
                if (payload.firstOrNull() == 0x00.toByte()) null else payload
        }
        val client = createClient(server.rpcClient, NotificationCodecs.of(dropZeroPrefixed))
        val typed = client.typed(KEYSPACE, ExampleLocker::toByteArray, ExampleLocker.Companion::fromByteArray)
        val roomId = randomRoomId()

        val ready = CompletableDeferred<Unit>()
        val received = CompletableDeferred<ByteArray>()
        val job = launch {
            client.lockers.notifications.onStart { ready.complete(Unit) }.first().let { received.complete(it.payload) }
        }

        ready.await()
        typed.subscribeToRoom(roomId)
        // First update is dropped by the codec; the flow only ever sees the second.
        typed.updateLocker(roomId, randomLockerId(KEYSPACE), { payload { rawValue = byteArrayOf(0x00, 0x63) } }) { it.copy { } }
        typed.updateLocker(roomId, randomLockerId(KEYSPACE), { payload { rawValue = byteArrayOf(0x01, 0x6B) } }) { it.copy { } }

        val payload = received.await()
        assertEquals(0x01.toByte(), payload.first())
        job.cancel()
        client.close()
    }

    @Test(timeout = 15_000)
    fun `a per-keyspace codec applies only to its keyspace`() = runTestWithServer(Application::attachTestServices) { server, _ ->
        // decode-only transform scoped to OTHER_KEYSPACE; the default keyspace is untouched.
        val codecs = NotificationCodecs.builder().add(OTHER_KEYSPACE, xorCodec(both = false)).build()
        val client = createClient(server.rpcClient, codecs)
        val typedDefault = client.typed(KEYSPACE, ExampleLocker::toByteArray, ExampleLocker.Companion::fromByteArray)
        val typedOther = client.typed(OTHER_KEYSPACE, ExampleLocker::toByteArray, ExampleLocker.Companion::fromByteArray)
        val roomId = randomRoomId()

        val ready = CompletableDeferred<Unit>()
        val defaultPayload = CompletableDeferred<ByteArray>()
        val otherPayload = CompletableDeferred<ByteArray>()
        val job = launch {
            typedDefault.notifications.onStart { ready.complete(Unit) }.first().let { defaultPayload.complete(it.payload) }
        }
        val job2 = launch {
            typedOther.notifications.first().let { otherPayload.complete(it.payload) }
        }

        ready.await()
        typedDefault.subscribeToRoom(roomId)
        typedOther.subscribeToRoom(roomId)

        typedDefault.updateLocker(roomId, randomLockerId(KEYSPACE), { payload { rawValue = byteArrayOf(0x10) } }) { it.copy { } }
        typedOther.updateLocker(roomId, randomLockerId(OTHER_KEYSPACE), { payload { rawValue = byteArrayOf(0x10) } }) { it.copy { } }

        // Default keyspace: no codec, payload unchanged.
        assertEquals(0x10.toByte(), defaultPayload.await().single())
        // Other keyspace: decode flips the bits.
        assertEquals((0x10 xor 0xFF).toByte(), otherPayload.await().single())
        job.cancel()
        job2.cancel()
        client.close()
    }
}
