package com.latenighthack.lockers.server

import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.Database
import com.latenighthack.lockers.common.LockerSigning
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.push.v1.*
import com.latenighthack.lockers.server.agents.LockerAgentRegistry
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.server.services.push.v1.*
import com.latenighthack.lockers.server.services.push.v1.providers.*
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.server.cluster.OwnerLifecycle
import com.latenighthack.lockers.sharding.*
import com.latenighthack.lockers.sharding.spi.*
import com.latenighthack.lockers.session.v1.*
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.*

/** Source content and ratchet authority must commit together. */
class RatchetAtomicityRegressionTest {
    @Test fun ratchetCannotSucceedWithoutACommittedAuthority(): Unit = runBlocking {
        val s = stores(); val impl = service(s); val rpc = LocalRoomServiceRpc(impl)
        val old = Secp256r1KeyPair.generate(); val next = Secp256r1KeyPair.generate()
        try {
            val response = rpc.postLockerChange(PostLockerChangeRequest(roomId = room, lockerId = id, locker = body(1),
                ratchet = PostLockerChangeRequest.Ratchet(newPublicKey = Secp256R1Key.PublicKey(next.publicKey.encode()),
                    signature = Signature(signature = old.privateKey.sign(LockerSigning.ratchetContext(room, id, 0, next.publicKey.encode()))))))
            assertFalse(response.result.isOk())
            assertNull(s.lockers.getLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(id.rawValue)))
        } finally { impl.close() }
    }
    private val room = RoomId(byteArrayOf(1))
    private val id = LockerId(byteArrayOf(2), LockerKeyspace(0))
    private fun body(n: Int) = Locker { open { encodedPayload = byteArrayOf(n.toByte()) } }
    private class Stores(val db: Database, val lockers: LockerStoreImpl, val locks: LockStoreImpl, val subs: SubscriptionStoreImpl, val outbox: DeliveryOutboxStore)
    private suspend fun stores(): Stores {
        val db = ServerStorage.inMemory()
        val s = Stores(db, LockerStoreImpl(db), LockStoreImpl(db), SubscriptionStoreImpl(db), DeliveryOutboxStore(db))
        s.lockers.prepare(); s.locks.prepare(); s.subs.prepare(); s.outbox.prepareStores(); db.open()
        return s
    }
    private fun service(s: Stores, fast: Boolean = false, lockers: LockerStore = s.lockers, discovery: SessionGatewayDiscovery = object : SessionGatewayDiscovery {
        override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = null
    }) = RoomServiceImpl(s.subs, lockers, s.locks, discovery, LocalRoomOwnership(), object : LockerAgentRegistry {
        override suspend fun processPayload(roomId: RoomId, lockerId: LockerId, locker: Locker) = emptyList<LockerAgentRegistry.LockerWrite>()
    }, SimpleMeterRegistry(), LockersConfig.defaults().copy(shardMultiplier = 0, roomWritesPerSecond = 0, deliveryOutboxEnabled = fast, deliveryWorkerEnabled = false), s.outbox)

    @Test fun legacySingleRatchetRollsBackWhenContentPersistenceFails(): Unit = runBlocking {
        val s = stores()
        val failing = object : LockerStore by s.lockers {
            override suspend fun updateLocker(locker: ServerLocker) { throw java.io.IOException("diagnostic save failure") }
            override suspend fun updateLockers(lockers: List<ServerLocker>) { lockers.forEach { updateLocker(it) } }
        }
        val impl = service(s, lockers = failing); val rpc = LocalRoomServiceRpc(impl)
        val old = Secp256r1KeyPair.generate(); val new = Secp256r1KeyPair.generate()
        val oldPublic = Secp256R1Key.PublicKey(old.publicKey.encode())
        val newPublic = new.publicKey.encode()
        try {
            assertTrue(rpc.lockLocker(LockLockerRequest(roomId = room, grant = LockGrant(
                scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM), publicKey = oldPublic))).result.isOk())
            val payload = byteArrayOf(10); val hash = LockVerifier(s.locks).contentHash(payload)
            val req = PostLockerChangeRequest(roomId = room, lockerId = id, parentVersion = 0,
                locker = Locker { sealed { this.payload { checksum = hash; enclosure { innerPayload = payload } } } },
                writeSignature = Signature(oldPublic, old.privateKey.sign(LockerSigning.writeContext(room, id, 0, hash))),
                ratchet = PostLockerChangeRequest.Ratchet(newPublicKey = Secp256R1Key.PublicKey(newPublic),
                    signature = Signature(oldPublic, old.privateKey.sign(LockerSigning.ratchetContext(room, id, 0, newPublic)))))
            try { rpc.postLockerChange(req); fail("save should fail") } catch (expected: java.io.IOException) { }
            assertContentEquals(old.publicKey.encode(), LockVerifier(s.locks).stateOf(LockVerifier(s.locks).resolveEffective(room, 0, id.rawValue)!!).publicKey!!.rawValue)
            assertNull(s.lockers.getLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(id.rawValue)))
        } finally { impl.close() }
    }

}
