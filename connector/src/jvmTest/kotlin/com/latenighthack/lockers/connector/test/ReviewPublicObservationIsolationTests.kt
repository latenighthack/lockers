package com.latenighthack.lockers.connector.test

import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.connector.storage.v1.*
import com.latenighthack.lockers.push.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.attachTestServices
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlin.test.*

class ReviewPublicObservationIsolationTests {
    @Test fun `public connection and session observations cannot change signed HTTP authority`() = runOwnedTestWithServer({ attachTestServices() }) { server, _ -> withContext(Dispatchers.Default) {
        val key = Secp256r1KeyPair.generate()
        val auth = object : AuthenticationKeySource {
            override suspend fun getSessionKeyPair() = key
            override suspend fun hasSessionKeyPair() = true
            override suspend fun generateSessionKeyPair() {}
            override suspend fun revokeKeys() {}
        }
        val client = createOwnedTestClient(server.ownedRpcClient, ConnectorStorage.inMemory(), KeyValueStore(InMemoryKeyValueStoreDelegate()), auth, Version())
        try {
            withTimeout(5_000) { client.awaitConnected() }
            val original = client.sessionId.value!!.rawValue.copyOf()
            (client.connection.value as StreamConnectionState.Connected).sessionId.rawValue.fill(0)
            (client.connection.replayCache.single() as StreamConnectionState.Connected).sessionId.rawValue.fill(0)
            (client.connection.first() as StreamConnectionState.Connected).sessionId.rawValue.fill(0)
            client.sessionId.value!!.rawValue.fill(0)
            client.sessionId.replayCache.single()!!.rawValue.fill(0)
            client.sessionId.first()!!.rawValue.fill(0)
            assertContentEquals(original, client.sessionId.value!!.rawValue)
            assertContentEquals(original, (client.connection.value as StreamConnectionState.Connected).sessionId.rawValue)
            withTimeout(5_000) { client.lockers.subscribeToRoom(RoomId(byteArrayOf(2))) }
        } finally { client.closeAndJoin() }
    } }

    @Test fun `push status observations detach session identities from subsequent RPC effects`() = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        val store = PushRegistrationStoreImpl(db).also { it.prepare() }; val session = SessionId(byteArrayOf(1))
        var calls = 0
        val controller = PushRegistrationController(ReviewRpc { _, bytes ->
            assertContentEquals(byteArrayOf(1), RegisterSessionRequest.fromByteArray(bytes).sessionId!!.rawValue)
            calls++; RegisterSessionResponse().toByteArray()
        }, store, MutableStateFlow(session))
        try {
            controller.register(PushRegistrations.fcm("one")); withTimeout(2_000) { controller.awaitRegistered(PushBackendType.FCM) }
            controller.registrations.value.getValue(PushBackendType.FCM).sessionId!!.rawValue.fill(0)
            controller.registrations.replayCache.single().getValue(PushBackendType.FCM).sessionId!!.rawValue.fill(0)
            controller.registrations.first().getValue(PushBackendType.FCM).sessionId!!.rawValue.fill(0)
            assertContentEquals(byteArrayOf(1), controller.registrations.value.getValue(PushBackendType.FCM).sessionId!!.rawValue)
            controller.register(PushRegistrations.fcm("two")); withTimeout(2_000) { controller.awaitRegistered(PushBackendType.FCM) }
            assertEquals(2, calls)
        } finally { controller.closeAndJoin(); db.close() }
    }

    @Test fun `subscription room events are detached per collector and cannot mutate reducer identity`() = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        val sessions = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db).also { it.prepare() }
        val subscriptions = SubscriptionStoreImpl(db).also { it.prepare() }; var calls = 0
        val controller = SubscriptionController(ReviewRpc { _, bytes -> calls++; SubscriptionResponse(currentRevision = SubscriptionRequest.fromByteArray(bytes).intentRevision).toByteArray() },
            subscriptions, sessions, MutableStateFlow(SessionId(byteArrayOf(1))), supportsRevisions = { true })
        try {
            val first = async(start = CoroutineStart.UNDISPATCHED) { controller.watchNewSubscriptions().first() }
            val second = async(start = CoroutineStart.UNDISPATCHED) { controller.watchNewSubscriptions().first() }
            controller.subscribe(RoomId(byteArrayOf(2)))
            withTimeout(2_000) { first.await() }.rawValue.fill(0)
            assertContentEquals(byteArrayOf(2), withTimeout(2_000) { second.await() }.rawValue)
            withTimeout(2_000) { controller.awaitSubscription(RoomId(byteArrayOf(2))) }
            controller.subscribe(RoomId(byteArrayOf(2))); assertEquals(1, calls)
        } finally { controller.closeAndJoin(); db.close() }
    }

    @Test fun `subscription input and output arrays preserve stored identity and unknown body bytes`() = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open(); val store = SubscriptionStoreImpl(db).also { it.prepare() }
        val unknown = byteArrayOf(0x98.toByte(), 0x06, 1)
        val input = StoredSubscription(roomIdRawValue = byteArrayOf(2), isPendingAdd = true, unknownFields = unknown.copyOf())
        try {
            store.updateSubscription(input)
            val expected = input.toByteArray()
            input.roomIdRawValue.fill(0); input.unknownFields!!.fill(0)
            val direct = store.getSubscription(RoomId(byteArrayOf(2)))!!
            assertContentEquals(expected, direct.toByteArray())
            direct.roomIdRawValue.fill(0); direct.unknownFields!!.fill(0)
            val listed = store.getAllSubscriptions().single(); assertContentEquals(expected, listed.toByteArray())
            listed.roomIdRawValue.fill(0); listed.unknownFields!!.fill(0)
            assertContentEquals(expected, store.getSubscription(RoomId(byteArrayOf(2)))!!.toByteArray())
            assertContentEquals(expected, store.getAllSubscriptions().single().toByteArray())
        } finally { db.close() }
    }
    @Test fun `session sequence and pending ACK observations detach all persisted arrays`() = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        val store = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db).also { it.prepare() }
        try {
            val sequence = byteArrayOf(1, 2, 3); store.updateNextSequenceBytes(sequence); sequence.fill(0)
            assertContentEquals(byteArrayOf(1, 2, 3), store.getNextSequenceBytes())
            store.getNextSequenceBytes()!!.fill(0); assertContentEquals(byteArrayOf(1, 2, 3), store.getNextSequenceBytes())
            val ack = StoredAck(roomIdRawValue = byteArrayOf(4), eventIdRawValue = byteArrayOf(5), unknownFields = byteArrayOf(0x98.toByte(), 6, 1))
            val expected = ack.toByteArray(); store.addAck(ack)
            ack.roomIdRawValue.fill(0); ack.eventIdRawValue.fill(0); ack.unknownFields!!.fill(0)
            val pending = store.getPendingAcks().single(); assertContentEquals(expected, pending.toByteArray())
            pending.roomIdRawValue.fill(0); pending.eventIdRawValue.fill(0); pending.unknownFields!!.fill(0)
            assertContentEquals(expected, store.pendingAckBatches().first().single().toByteArray())
            assertTrue(store.hasReceived(StoredAck(byteArrayOf(4), byteArrayOf(5))))
        } finally { db.close() }
    }
    @Test fun `push store registration and revision intent arrays cannot mutate persisted credentials`() = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open(); val store = PushRegistrationStoreImpl(db).also { it.prepare() }
        try {
            val encoded = PushRegistrations.fcm("one").toByteArray()
            val row = StoredPushRegistration(2, encoded.copyOf(), true, byteArrayOf(0x98.toByte(), 6, 1))
            val expected = row.toByteArray(); store.saveRegistration(row)
            row.encodedRegistration.fill(0); row.unknownFields!!.fill(0)
            val direct = store.getRegistration(2)!!; assertContentEquals(expected, direct.toByteArray())
            direct.encodedRegistration.fill(0); direct.unknownFields!!.fill(0)
            val listed = store.getAllRegistrations().single(); assertContentEquals(expected, listed.toByteArray())
            listed.encodedRegistration.fill(0); listed.unknownFields!!.fill(0)
            assertContentEquals(expected, store.getRegistration(2)!!.toByteArray())
            val intent = store.nextIntent(2, encoded); intent.encodedRegistration.fill(0)
            assertContentEquals(encoded, store.getAllIntents().single().encodedRegistration)
            store.getAllIntents().single().encodedRegistration.fill(0)
            val saved = store.getAllIntents().single(); assertContentEquals(encoded, saved.encodedRegistration)
            assertTrue(store.confirmIntent(saved)); assertFalse(store.getAllIntents().single().pending)
        } finally { db.close() }
    }

}
