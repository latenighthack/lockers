package com.latenighthack.lockers.server

import com.latenighthack.ktstore.Database
import com.latenighthack.ktcrypto.encode
import com.latenighthack.ktcrypto.generate
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.agents.LockerAgentRegistry
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.session.v1.*
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.*
import kotlin.test.*

class DeliveryOutboxTest {
    @Test fun slowGatewayDoesNotBlockNewWorkForAnotherRoom() = runBlocking {
        val db = com.latenighthack.lockers.server.ServerStorage.inMemory()
        val outbox = DeliveryOutboxStore(db).also { it.prepareStores() }
        db.open()
        val slowEntered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val fastDelivered = CompletableDeferred<Unit>()
        val gateway = object : SessionGatewayService {
            override suspend fun postEvent(request: PostEventRequest): PostEventResponse {
                if (request.sessionIds.first().rawValue[0].toInt() == 0) { slowEntered.complete(Unit); release.await() }
                else fastDelivered.complete(Unit)
                return PostEventResponse()
            }
            override suspend fun postEvents(request: PostEventsRequest) = PostEventsResponse(request.groups.map { postEvent(it) })
        }
        val worker = DeliveryWorker(outbox, object : SessionGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): SessionGatewayService = gateway
        })
        worker.start()
        try {
            outbox.commit(room, listOf(SessionId(byteArrayOf(0))), listOf(event(1))) { }
            withTimeout(1000) { slowEntered.await() }
            val otherRoom = RoomId(byteArrayOf(2))
            outbox.commit(otherRoom, listOf(SessionId(byteArrayOf(1))), listOf(event(2).copy(roomId = otherRoom))) { }
            withTimeout(1000) { fastDelivered.await() }
        } finally { release.complete(Unit); worker.stop() }
    }

    @Test fun lostGatewayAcknowledgementReplaysOneDurableInboxEntry() = runBlocking {
        val db = com.latenighthack.lockers.server.ServerStorage.inMemory()
        val outbox = DeliveryOutboxStore(db).also { it.prepareStores() }
        val inbox = SessionInboxStoreImpl(db).also { it.prepare() }
        val sessions = SessionStoreImpl(db).also { it.prepare() }
        db.open()
        val service = SessionServiceImpl(sessions, inbox, SimpleMeterRegistry(), object : com.latenighthack.lockers.server.services.push.v1.PushGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): com.latenighthack.lockers.push.v1.PushGatewayService? = null
        }, LocalSessionOwnership(), LockersConfig.defaults())
        val real = LocalSessionGatewayServiceRpc(service)
        var attempts = 0
        val unreliable = object : SessionGatewayService by real {
            override suspend fun postEvents(request: PostEventsRequest): PostEventsResponse {
                val result = real.postEvents(request)
                if (++attempts == 1) throw java.io.IOException("gateway died after inbox insertion")
                return result
            }
        }
        val worker = DeliveryWorker(outbox, object : SessionGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): SessionGatewayService = unreliable
        })
        val id = SessionId(byteArrayOf(3))
        sessions.updateSession(ServerSession(sessionId = ServerSessionId(id.rawValue), nextKeyMaterial = ByteArray(32),
            authorizedPublicKey = com.latenighthack.ktcrypto.Secp256r1KeyPair.generate().publicKey.encode()))
        try {
            outbox.commit(room, listOf(id), listOf(event(1))) { }
            worker.drainOnce()
            assertEquals(1, outbox.pendingCount())
            withTimeout(5000) { while (outbox.pendingCount() > 0) { delay(20); worker.drainOnce() } }
            assertEquals(2, attempts)
            assertEquals(1, inbox.getAllEvents(ServerSessionId(id.rawValue)).size)
        } finally { worker.close(); service.close() }
    }

    private val room = RoomId(byteArrayOf(1))
    private fun event(id: Int) = Event(roomId = room, eventId = EventId(byteArrayOf(id.toByte())))

    @Test fun commitRollbackLeaseRecoveryAndRecipientProgress() = runBlocking {
        val db = com.latenighthack.lockers.server.ServerStorage.inMemory()
        val outbox = DeliveryOutboxStore(db).also { it.prepareStores() }
        val lockers = LockerStoreImpl(db).also { it.prepare() }
        db.open()
        val a = SessionId(byteArrayOf(1)); val b = SessionId(byteArrayOf(2))
        assertFailsWith<IllegalStateException> {
            outbox.commit(room, listOf(a, b), listOf(event(1))) {
                lockers.updateLocker(ServerLocker(ServerRoomId(room.rawValue), 1, ServerLockerId(byteArrayOf(1))))
                error("crash before commit")
            }
        }
        assertEquals(0, outbox.pendingCount())
        assertTrue(lockers.getAllLockers(ServerRoomId(room.rawValue)).isEmpty())
        outbox.commit(room, listOf(a, b), listOf(event(1), event(2))) { }
        val first = outbox.claim("dead-process", 100, 1000).single()
        assertEquals(1, first.roomSequence)
        assertTrue(outbox.claim("another-process", 500).isEmpty())
        val recovered = outbox.claim("another-process", 1100).single()
        assertEquals(first.eventId, recovered.eventId)
        outbox.accepted(first, listOf(a, b)) // stale lease cannot acknowledge new owner work
        outbox.accepted(recovered, listOf(a))
        assertEquals(2, outbox.pendingCount())
        outbox.accepted(recovered, listOf(b))
        assertEquals(2, outbox.claim("another-process", 1101).single().roomSequence)
    }

    @Test fun gatewayCallsScaleWithNodesAndWritesNeverWaitForDelivery() = runBlocking {
        for (count in listOf(1, 10, 100)) {
            val db = com.latenighthack.lockers.server.ServerStorage.inMemory()
            val outbox = DeliveryOutboxStore(db).also { it.prepareStores() }
            db.open()
            var calls = 0
            val gateway = object : SessionGatewayService {
                override suspend fun postEvent(request: PostEventRequest): PostEventResponse { calls++; return PostEventResponse() }
                override suspend fun postEvents(request: PostEventsRequest) = PostEventsResponse(request.groups.map { postEvent(it) })
            }
            val discovery = object : SessionGatewayDiscovery {
                override suspend fun findServer(sessionId: SessionId): SessionGatewayService = error("must resolve in bulk")
                override suspend fun resolveGroups(sessionIds: List<SessionId>) = sessionIds.groupBy { it.rawValue[0].toInt() % 2 }.values.map { SessionGatewayGroup(it, gateway) }
            }
            outbox.commit(room, (0 until count).map { SessionId(byteArrayOf(it.toByte())) }, listOf(event(1))) { }
            assertEquals(0, calls)
            val worker = DeliveryWorker(outbox, discovery)
            worker.drainOnce()
            assertEquals(minOf(count, 2), calls)
            assertEquals(0, outbox.pendingCount())
            worker.close()
        }
    }

    @Test fun atomicConflictRollsBackAndAcceptedReplayDoesNotRerunAgent() = runBlocking {
        val db = com.latenighthack.lockers.server.ServerStorage.inMemory()
        val outbox = DeliveryOutboxStore(db).also { it.prepareStores() }
        val lockers = LockerStoreImpl(db).also { it.prepare() }
        val locks = LockStoreImpl(db).also { it.prepare() }
        val subs = SubscriptionStoreImpl(db).also { it.prepare() }
        db.open()
        var agentCalls = 0
        val agent = object : LockerAgentRegistry {
            override suspend fun processPayload(roomId: RoomId, lockerId: LockerId, locker: Locker): List<LockerAgentRegistry.LockerWrite> {
                agentCalls++; return emptyList()
            }
        }
        val discovery = object : SessionGatewayDiscovery { override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = error("write must not deliver") }
        val service = RoomServiceImpl(subs, lockers, locks, discovery, object : RoomOwnership {
            override suspend fun resolve(keyspace: Long, roomId: RoomId) = RoomOwner.Local()
        }, agent, SimpleMeterRegistry(), LockersConfig.defaults().copy(deliveryOutboxEnabled = true, deliveryWorkerEnabled = false), outbox)
        val rpc = LocalRoomServiceRpc(service)
        try {
            fun change(id: Int, version: Long = 0) = PostLockerChangeRequest(roomId = room, lockerId = LockerId(byteArrayOf(id.toByte()), LockerKeyspace(1)),
                locker = Locker { open { encodedPayload = byteArrayOf(42) } }, parentVersion = version)
            val request = PostLockerChangesRequest(roomId = room, changes = listOf(change(1), change(2)), writeRequestId = ByteArray(16) { 1 })
            val accepted = rpc.postLockerChanges(request)
            assertTrue(accepted.result.isOk())
            assertEquals(2, agentCalls)
            assertEquals(accepted, rpc.postLockerChanges(request))
            assertEquals(2, agentCalls)
            assertTrue(rpc.postLockerChanges(request.copy(changes = listOf(change(3)))).result is PostLockerChangesResponse.Result.REQUEST_ID_REUSED)
            val conflict = rpc.postLockerChanges(request.copy(writeRequestId = ByteArray(16) { 2 }, changes = listOf(change(3), change(1, 99))))
            assertTrue(conflict.result is PostLockerChangesResponse.Result.CONFLICT)
            assertNull(lockers.getLocker(ServerRoomId(room.rawValue), 1, ServerLockerId(byteArrayOf(3))))
            assertEquals(2, agentCalls)
        } finally { service.close() }
    }
}
