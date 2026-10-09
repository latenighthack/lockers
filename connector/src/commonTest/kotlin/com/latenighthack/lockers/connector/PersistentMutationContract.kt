package com.latenighthack.lockers.connector

import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.internal.*
import com.latenighthack.lockers.connector.storage.v1.*
import com.latenighthack.lockers.room.v1.*
import kotlinx.coroutines.flow.first
import kotlin.test.*

/** SDK domain mutations exercise nested logical owners on the actual persistent backend. */
suspend fun verifyPersistentConnectorMutations(factory: () -> Database) {
    val room = RoomId(byteArrayOf(1)); val id = LockerId(byteArrayOf(2), LockerKeyspace(4))
    val event = Event(roomId = room, eventId = EventId(byteArrayOf(3)), notification = Notification {
        payload { rawValue = byteArrayOf(4) }; push { title = "title"; body = "body" }
    })
    val stored = StoredLocker(roomIdRawValue = room.rawValue, lockerIdRawValue = id.rawValue,
        lockerKeyspace = 4, lockerPayload = byteArrayOf(5), version = 11)
    val key = Secp256r1KeyPair.generate()
    val request = PostLockerChangeRequest(roomId = room, lockerId = id, writeRequestId = ByteArray(32) { 6 },
        ratchet = PostLockerChangeRequest.Ratchet(newPublicKey = Secp256R1Key.PublicKey(key.publicKey.encode())))
    val pending = PendingRatchet(request, key.privateKey.encode(), RatchetExpectation(request.writeRequestId,
        LockState(locked = true, lockVersion = 2, scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_LOCKER,
            keyspace = id.keyspace, lockerRawValue = id.rawValue), publicKey = request.ratchet!!.newPublicKey)))
    var db = factory(); db.open()
    try {
        val store = LockerStoreImpl(db)
        val session = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db)
        session.receive(event) { store.acceptLocker(stored); true }
        session.receive(event) { fail("Duplicate event was accepted twice") }
        val rejected = event.copy(eventId = EventId(byteArrayOf(7)))
        assertFailsWith<IllegalStateException> { session.receive(rejected) {
            store.acceptLocker(stored.copy(version = 12)); false
        } }
        assertEquals(11, store.getLocker(room, id.keyspace!!, id)!!.version)
        assertFalse(session.hasReceived(StoredAck(room.rawValue, rejected.eventId!!.rawValue)))
        session.clearAck(StoredAck(room.rawValue, event.eventId!!.rawValue))
        store.saveRatchet(pending); store.acknowledgeRatchetUncertainty(request)
        val push = PushRegistrationStoreImpl(db)
        val intent = push.nextIntent(1, byteArrayOf(8, 9))
        assertTrue(push.confirmIntent(intent))
    } finally { db.close() }
    db = factory(); db.open()
    try {
        val store = LockerStoreImpl(db)
        val session = SessionStoreImpl(KeyValueStore(InMemoryKeyValueStoreDelegate()), db)
        assertEquals(11, store.getLocker(room, id.keyspace!!, id)!!.version)
        assertTrue(session.getPendingAcks().isEmpty())
        assertTrue(session.hasReceived(StoredAck(room.rawValue, event.eventId!!.rawValue)))
        assertContentEquals(event.toByteArray(), session.eventsAfter(0).first().payload)
        assertEquals(11, StoredLocker.fromByteArray(store.changesAfter(0).first().payload).version)
        val recovered = store.pendingRatchets().single()
        assertContentEquals(pending.privateKey, recovered.privateKey)
        assertContentEquals(request.toByteArray(), recovered.request.toByteArray())
        assertTrue(recovered.expectation!!.sourceUncertaintyAcknowledged)
        val intent = PushRegistrationStoreImpl(db).getAllIntents().single()
        assertEquals(1, intent.revision); assertFalse(intent.pending)
        assertContentEquals(byteArrayOf(8, 9), intent.encodedRegistration)
    } finally { db.close() }
}
