package com.latenighthack.lockers.server

import com.latenighthack.ktstore.Database
import com.latenighthack.lockers.common.v1.Event
import com.latenighthack.lockers.common.v1.Locker
import com.latenighthack.lockers.common.v1.LockerId
import com.latenighthack.lockers.common.v1.LockerKeyspace
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.common.v1.fromByteArray
import com.latenighthack.lockers.common.v1.toByteArray
import com.latenighthack.lockers.room.v1.GetWriteOutcomeRequest
import com.latenighthack.lockers.room.v1.LocalRoomServiceRpc
import com.latenighthack.lockers.room.v1.PostLockerChangeRequest
import com.latenighthack.lockers.room.v1.PostLockerChangesRequest
import com.latenighthack.lockers.room.v1.PostLockerChangesResponse
import com.latenighthack.lockers.room.v1.WriteOutcome
import com.latenighthack.lockers.server.agents.LockerAgentRegistry
import com.latenighthack.lockers.server.agents.TransactionalLockerAgentRegistry
import com.latenighthack.lockers.server.services.room.v1.DeliveryOutboxStore
import com.latenighthack.lockers.server.services.room.v1.LocalRoomOwnership
import com.latenighthack.lockers.server.services.room.v1.LockStoreImpl
import com.latenighthack.lockers.server.services.room.v1.LockerStoreImpl
import com.latenighthack.lockers.server.services.room.v1.RoomServiceImpl
import com.latenighthack.lockers.server.services.room.v1.SubscriptionStoreImpl
import com.latenighthack.lockers.server.services.session.v1.SessionGatewayDiscovery
import com.latenighthack.lockers.server.storage.v1.ServerLocker
import com.latenighthack.lockers.server.storage.v1.ServerLockerId
import com.latenighthack.lockers.server.storage.v1.ServerRoomId
import com.latenighthack.lockers.server.storage.v1.ServerSessionId
import com.latenighthack.lockers.session.v1.SessionGatewayService
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** A transactional agent's frames and state commit in the source write's own room transaction. */
class InlineAgentTest {
    private val room = RoomId(byteArrayOf(4, 4))
    private val serverRoom = ServerRoomId(room.rawValue)
    private val source = LockerId(byteArrayOf(1))
    private val frame = LockerId(byteArrayOf(1), LockerKeyspace(30))
    private val stateKeyspace = 99L
    private fun body(n: Int) = Locker { open { encodedPayload = byteArrayOf(n.toByte()) } }
    private fun move(n: Int, parent: Long) = PostLockerChangesRequest(roomId = room,
        writeRequestId = ByteArray(16) { n.toByte() },
        changes = listOf(PostLockerChangeRequest(roomId = room, lockerId = source, locker = body(n),
            parentVersion = parent)))

    /** Writes one frame per source write and records agent state through its commit hook. */
    private inner class GameAgent(val lockers: () -> LockerStoreImpl) : TransactionalLockerAgentRegistry {
        val prepared = AtomicInteger(); val committed = AtomicInteger()
        var failPrepare = false; var failCommit = false
        override suspend fun prepare(roomId: RoomId, lockerId: LockerId,
            locker: Locker): TransactionalLockerAgentRegistry.Prepared {
            prepared.incrementAndGet()
            check(!failPrepare) { "driver unavailable" }
            return TransactionalLockerAgentRegistry.Prepared(listOf(LockerAgentRegistry.LockerWrite(frame, locker))) {
                check(!failCommit) { "agent state rejected" }
                val stored = lockers().getLocker(serverRoom, stateKeyspace, ServerLockerId(lockerId.rawValue))
                lockers().updateLocker(ServerLocker(serverRoom, stateKeyspace, ServerLockerId(lockerId.rawValue),
                    locker.toByteArray(), (stored?.version ?: 0) + 1))
                committed.incrementAndGet()
            }
        }
    }

    private class Harness(val db: Database, val outbox: DeliveryOutboxStore, val lockers: LockerStoreImpl,
        val service: RoomServiceImpl) {
        val rpc = LocalRoomServiceRpc(service)
        suspend fun close() { service.closeAndJoin(); db.close() }
    }

    private suspend fun harness(agent: (() -> LockerStoreImpl) -> TransactionalLockerAgentRegistry):
        Pair<Harness, GameAgent> {
        val db = ServerStorage.inMemory()
        val outbox = DeliveryOutboxStore(db).also { it.prepareStores() }
        val lockers = LockerStoreImpl(db).also { it.prepare() }
        val locks = LockStoreImpl(db).also { it.prepare() }
        val subs = SubscriptionStoreImpl(db).also { it.prepare() }
        db.open()
        subs.addSubscription(ServerSessionId(byteArrayOf(7)), serverRoom)
        val registry = agent { lockers }
        val noGateway = object : SessionGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = null
        }
        val config = LockersConfig.defaults().copy(deliveryWorkerEnabled = false)
        val service = RoomServiceImpl(subs, lockers, locks, noGateway, LocalRoomOwnership(), registry,
            SimpleMeterRegistry(), config, outbox)
        service.start()
        return Harness(db, outbox, lockers, service) to registry as GameAgent
    }
    private suspend fun Harness.locker(id: LockerId) =
        lockers.getLocker(serverRoom, id.keyspace?.value ?: 0, ServerLockerId(id.rawValue))
    private suspend fun Harness.events() = outbox.claim("test", System.currentTimeMillis(), eventsPerRoom = 64)
        .map { Event.fromByteArray(it.encodedEvent) }

    @Test fun `source write frames and agent state commit together and settle in the response`(): Unit = runBlocking {
        val (h, agent) = harness { GameAgent(it) }
        try {
            val response = h.rpc.postLockerChanges(move(1, 0))
            assertEquals(PostLockerChangesResponse.Result.OK, response.result)
            assertFalse(response.agentPending, "frames committed with the move")
            // The reply carries the committed frame exactly as subscribers will receive it.
            val returned = response.derivedLockers.single()
            assertEquals(1L, returned.version)
            assertEquals(30L, returned.lockerId?.keyspace?.value)
            assertEquals(body(1).toByteArray().toList(), requireNotNull(returned.locker).toByteArray().toList())
            assertEquals(1, agent.prepared.get()); assertEquals(1, agent.committed.get())
            assertEquals(1L, h.locker(source)?.version); assertEquals(1L, h.locker(frame)?.version)
            assertEquals(1L, h.locker(LockerId(source.rawValue, LockerKeyspace(stateKeyspace)))?.version)
            // Source then derived event, consecutive room sequences from the same commit.
            val sequenced = h.events().map { event ->
                val id = requireNotNull(event.locker?.lockerId)
                LockerId(id.rawValue, id.keyspace?.takeIf { it.value != 0L }) to event.roomSequence
            }
            assertEquals(listOf(source to 1L, frame to 2L), sequenced)
            val outcome = h.rpc.getWriteOutcome(GetWriteOutcomeRequest(room, move(1, 0).writeRequestId)).outcome
            assertEquals(WriteOutcome.AgentState.APPLIED, outcome?.agentState)
            // A replay is answered from the receipt and journal; agent state never commits twice.
            val replay = h.rpc.postLockerChanges(move(1, 0))
            assertFalse(replay.agentPending)
            assertTrue(replay.derivedLockers.isEmpty(), "replays fall back to delivery for frames")
            assertEquals(1, agent.committed.get())
            assertEquals(1L, h.locker(frame)?.version)
        } finally { h.close() }
    }

    @Test fun `a rejected write never commits prepared agent state`(): Unit = runBlocking {
        val (h, agent) = harness { GameAgent(it) }
        try {
            h.rpc.postLockerChanges(move(1, 0))
            val conflict = h.rpc.postLockerChanges(move(2, 0))
            assertEquals(PostLockerChangesResponse.Result.CONFLICT, conflict.result)
            assertEquals(1, agent.committed.get())
            assertEquals(1L, h.locker(frame)?.version)
        } finally { h.close() }
    }

    @Test fun `a failed preparation falls back to the journaled agent path`(): Unit = runBlocking {
        val (h, agent) = harness { GameAgent(it) }
        try {
            // Preparation fails inline and on the first journaled attempt, then succeeds on retry.
            agent.failPrepare = true
            val response = h.rpc.postLockerChanges(move(1, 0))
            assertEquals(PostLockerChangesResponse.Result.OK, response.result)
            assertTrue(response.agentPending)
            assertTrue(response.derivedLockers.isEmpty())
            assertNull(h.locker(frame))
            agent.failPrepare = false
            val outcome = withTimeout(10_000) { h.service.writeOutcomeFlow(room, move(1, 0).writeRequestId)
                .let { flow -> var last: WriteOutcome? = null; flow.collect { last = it }; last } }
            assertEquals(WriteOutcome.AgentState.APPLIED, outcome?.agentState)
            assertEquals(1L, h.locker(frame)?.version)
            assertEquals(1, agent.committed.get())
        } finally { h.close() }
    }

    @Test fun `prepared output is discarded while earlier agent work is queued`(): Unit = runBlocking {
        val (h, agent) = harness { GameAgent(it) }
        try {
            agent.failPrepare = true
            h.rpc.postLockerChanges(move(1, 0))
            agent.failPrepare = false
            // The second move prepares inline, but the first move's frames must apply first.
            val second = h.rpc.postLockerChanges(move(2, 1))
            assertEquals(PostLockerChangesResponse.Result.OK, second.result)
            withTimeout(10_000) {
                val request = GetWriteOutcomeRequest(room, move(2, 1).writeRequestId)
                while (h.rpc.getWriteOutcome(request).outcome?.agentState != WriteOutcome.AgentState.APPLIED) delay(20)
            }
            val frames = h.events().filter { it.locker?.lockerId?.keyspace?.value == 30L }
            assertEquals(listOf(1L, 2L), frames.map { it.locker?.version })
            val latest = Locker.fromByteArray(requireNotNull(h.locker(frame)).locker)
            assertEquals(byteArrayOf(2).toList(), latest.open?.encodedPayload?.toList())
        } finally { h.close() }
    }

    @Test fun `a failing commit hook rolls back the whole move`(): Unit = runBlocking {
        val (h, agent) = harness { GameAgent(it) }
        try {
            agent.failCommit = true
            assertFailsWith<IllegalStateException> { h.rpc.postLockerChanges(move(1, 0)) }
            assertNull(h.locker(source)); assertNull(h.locker(frame))
            assertNull(h.outbox.receipt(room, move(1, 0).writeRequestId))
            assertTrue(h.events().isEmpty())
            agent.failCommit = false
            assertNotNull(h.rpc.postLockerChanges(move(1, 0)).takeIf { !it.agentPending })
        } finally { h.close() }
    }
}
