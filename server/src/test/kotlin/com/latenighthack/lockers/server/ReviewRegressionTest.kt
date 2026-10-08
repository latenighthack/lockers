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

/** Security regressions for the reviewed trust boundaries. */
class ReviewRegressionTest {
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

    @Test fun ambiguousEnvelopeIsRejectedWithoutPersistingUnsignedContent(): Unit = runBlocking {
        val s = stores(); val service = service(s); val rpc = LocalRoomServiceRpc(service)
        val key = Secp256r1KeyPair.generate()
        val public = Secp256R1Key.PublicKey(key.publicKey.encode())
        try {
            assertTrue(rpc.lockLocker(LockLockerRequest(roomId = room, grant = LockGrant(
                scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM), publicKey = public))).result.isOk())
            val authentic = byteArrayOf(10)
            val hash = LockVerifier(s.locks).contentHash(authentic)
            val signature = Signature(public, key.privateKey.sign(LockerSigning.writeContext(room, id, 0, hash)))
            val mixed = Locker {
                sealed { payload { checksum = hash; enclosure { innerPayload = authentic } } }
                open { encodedPayload = byteArrayOf(99) } // appended without signing
            }
            val result = rpc.postLockerChange(PostLockerChangeRequest(roomId = room, lockerId = id,
                locker = mixed, parentVersion = 0, writeSignature = signature))
            assertFalse(result.result.isOk())
            assertNull(s.lockers.getLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(id.rawValue)))
        } finally { service.close() }
    }
    @Test fun newAuthorityIsEnforcedAfterAnotherReplicaReadAnOpenRoom(): Unit = runBlocking {
        val s = stores(); val a = service(s); val b = service(s)
        val ra = LocalRoomServiceRpc(a); val rb = LocalRoomServiceRpc(b)
        try {
            rb.postLockerChange(PostLockerChangeRequest(roomId = room, lockerId = id, locker = body(1)))
            ra.getLocker(GetLockerRequest(room, id)) // caches no locks on a non-owning reader
            val key = Secp256r1KeyPair.generate()
            assertTrue(rb.lockLocker(LockLockerRequest(roomId = room, grant = LockGrant(
                scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM), publicKey = Secp256R1Key.PublicKey(key.publicKey.encode())))).result.isOk())
            assertFalse(ra.postLockerChange(PostLockerChangeRequest(roomId = room, lockerId = id, locker = body(2))).result.isOk())
        } finally { a.close(); b.close() }
    }

}
