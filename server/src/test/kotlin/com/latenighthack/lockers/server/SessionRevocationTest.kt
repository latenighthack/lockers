package com.latenighthack.lockers.server

import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.SessionSigning
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.session.v1.*
import kotlinx.coroutines.runBlocking
import kotlin.random.Random
import kotlin.test.*

class SessionRevocationTest {
    @Test fun authenticatedDestroyRemovesAuthorityAndReservesTheId(): Unit = runBlocking {
        val db = ServerStorage.inMemory()
        val core = ServerCore::class.create(LockersConfig.defaults().copy(deliveryWorkerEnabled = false, pushWorkerEnabled = false), db)
        core.setup()
        val component = MonolithComponent(core)
        val key = Secp256r1KeyPair.generate()
        val sid = SessionId(byteArrayOf(41))
        val stored = ServerSession(ServerSessionId(sid.rawValue), byteArrayOf(42), key.publicKey.encode())
        val event = ServerSessionEvent(ServerSessionId(sid.rawValue), ServerRoomId(byteArrayOf(43)), ServerEventId(byteArrayOf(44)), byteArrayOf(45))
        core.sessionStore.updateSession(stored)
        core.subscriptionStore.addSubscription(requireNotNull(stored.sessionId), requireNotNull(event.roomId))
        core.sessionInboxStore.saveEvent(event)
        core.pushStore.savePushInfo(ServerPushInfo(requireNotNull(stored.sessionId)))
        core.pushQueueStore.savePush(ServerPush(ServerPushId(byteArrayOf(46)), requireNotNull(stored.sessionId), 1, byteArrayOf(47)))
        val snapshots = com.latenighthack.lockers.server.services.room.v1.SnapshotStore(db, core.config.resourceLimits)
        val snapshotRoom = RoomId(byteArrayOf(49))
        val page = snapshots.create(1, snapshotRoom, sid, setOf(0), 1, 1, (1..2).map {
            IdentifiedLocker(LockerId(byteArrayOf(it.toByte())), Locker { open { encodedPayload = byteArrayOf(1) } }, 1)
        })
        val request = DestroySessionRequest(sessionId = sid)
        val now = System.currentTimeMillis(); val nonce = Random.nextBytes(32)
        val signed = request.copy(proof = SessionProof(now, nonce, Signature(signature = key.privateKey.sign(
            SessionSigning.context(SessionSigning.DESTROY, sid, SHA256.digest(request.toByteArray()), now, nonce)), signingVersion = 2)))
        try {
            val rpc = LocalSessionServiceRpc(component.sessionServiceModule.server)
            assertFalse(rpc.destroySession(request).result.isOk())
            assertNotNull(core.sessionStore.getSessionById(requireNotNull(stored.sessionId)))
            assertTrue(rpc.destroySession(signed).result.isOk())
            assertFailsWith<com.latenighthack.ktbuf.net.RpcResponseException> {
                snapshots.next(1, snapshotRoom, sid, setOf(0), 1, page.nextPageToken)
            }
            assertNull(core.sessionStore.getSessionById(requireNotNull(stored.sessionId)))
            assertTrue(core.subscriptionStore.getAllSubscriptions(requireNotNull(stored.sessionId)).isEmpty())
            assertTrue(core.sessionInboxStore.getAllEvents(requireNotNull(stored.sessionId)).isEmpty())
            assertNull(core.pushStore.getPushInfo(requireNotNull(stored.sessionId)))
            assertTrue(core.pushQueueStore.getPendingPushes().isEmpty())
            assertFalse(core.sessionStore.createIfAbsent(stored), "Destroyed IDs must never be reused by queued old deliveries")
            assertFalse(rpc.destroySession(signed).result.isOk())
        } finally { component.stop(); db.close() }
    }
}
