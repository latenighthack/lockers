package com.latenighthack.lockers.server

import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.agents.*
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.server.storage.v2.*
import com.latenighthack.lockers.session.v1.*
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import com.latenighthack.lockers.observability.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.*

class AgentWorkflowTest {
    private val room = RoomId(byteArrayOf(1))
    private val sourceId = LockerId(byteArrayOf(2))
    private val derivedId = LockerId(byteArrayOf(3))
    private fun body(n: Int) = Locker { open { encodedPayload = byteArrayOf(n.toByte()) } }
    private fun request(id: Int, parent: Long = 0) = PostLockerChangesRequest(roomId = room, writeRequestId = ByteArray(16) { id.toByte() }, changes = listOf(
        PostLockerChangeRequest(roomId = room, lockerId = sourceId, locker = body(id), parentVersion = parent)))
    private class Harness(val db: com.latenighthack.ktstore.Database, val outbox: DeliveryOutboxStore, val lockers: LockerStoreImpl, val service: RoomServiceImpl) {
        val rpc = LocalRoomServiceRpc(service)
        suspend fun close() { service.closeAndJoin(); db.close() }
    }
    private suspend fun harness(agent: LockerAgentRegistry, ownership: RoomOwnership = LocalRoomOwnership(), timeoutMs: Long = 30_000, maxAttempts: Int = 8, telemetry: LockersTelemetry = LockersTelemetry.NONE): Harness {
        val db = ServerStorage.inMemory()
        val outbox = DeliveryOutboxStore(db).also { it.prepareStores() }
        val lockers = LockerStoreImpl(db).also { it.prepare() }
        val locks = LockStoreImpl(db).also { it.prepare() }
        val subs = SubscriptionStoreImpl(db).also { it.prepare() }
        db.open()
        return Harness(db, outbox, lockers, RoomServiceImpl(subs, lockers, locks, object: SessionGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = null
        }, ownership, agent, SimpleMeterRegistry(), LockersConfig.defaults().copy(deliveryWorkerEnabled = false), outbox, telemetry = telemetry, agentTimeoutMs = timeoutMs, agentMaxAttempts = maxAttempts))
    }
    private fun durable(calls: AtomicInteger = AtomicInteger(), block: suspend (ByteArray) -> List<LockerAgentRegistry.LockerWrite> = { emptyList() }) = object: IdempotentLockerAgentRegistry {
        override val agentVersion = "fixture/v1"
        override suspend fun processPayload(effectKey: ByteArray, roomId: RoomId, lockerId: LockerId, locker: Locker): List<LockerAgentRegistry.LockerWrite> {
            calls.incrementAndGet(); return block(effectKey)
        }
    }
    private suspend fun awaitState(h: Harness, req: PostLockerChangesRequest, state: WriteOutcome.AgentState): WriteOutcome = withTimeout(3000) {
        while (true) {
            val outcome = h.rpc.getWriteOutcome(GetWriteOutcomeRequest(room, req.writeRequestId)).outcome!!
            if (outcome.agentState == state) return@withTimeout outcome
            delay(20)
        }
        error("unreachable")
    }
    @Test fun `expired idempotent execution reuses immutable effect identity and applies once`(): Unit = runBlocking {
        val calls = AtomicInteger(); val effects = java.util.concurrent.ConcurrentHashMap.newKeySet<String>()
        val agent = durable(calls) { effects.add(it.joinToString()); listOf(LockerAgentRegistry.LockerWrite(derivedId, body(7))) }
        val h = harness(agent); val req = request(1)
        try {
            val response = h.rpc.postLockerChanges(req)
            assertTrue(response.agentPending); assertContentEquals(req.writeRequestId, response.writeRequestId)
            assertEquals(sourceId, response.sourceVersions.single().lockerId)
            val dead = h.outbox.agentWork.claim(room, req.writeRequestId, agent.agentVersion, 1000, 100)!!
            agent.processPayload(SHA256.digest(dead.effectKey + java.nio.ByteBuffer.allocate(4).putInt(0).array()), room, sourceId, body(1))
            h.outbox.initializeAgentReceipts()
            h.service.start()
            awaitState(h, req, WriteOutcome.AgentState.APPLIED)
            assertEquals(2, calls.get()); assertEquals(1, effects.size)
            assertEquals(1, h.lockers.getLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(derivedId.rawValue))!!.version)
            assertNull(h.outbox.agentWork.result(dead, emptyList()))
            h.rpc.postLockerChanges(req)
            assertEquals(2, calls.get())
        } finally { h.close() }
    }
    @Test fun `persisted result recovers without invoking the external effect again`(): Unit = runBlocking {
        val calls = AtomicInteger(); val agent = durable(calls) { error("persisted result must not execute again") }
        val h = harness(agent); val req = request(2)
        try {
            h.rpc.postLockerChanges(req)
            val claim = h.outbox.agentWork.claim(room, req.writeRequestId, agent.agentVersion, System.currentTimeMillis(), 30_000)!!
            h.outbox.agentWork.result(claim, listOf(ServerAgentDerivedWrite(derivedId.rawValue, 0, body(8).toByteArray())))
            h.outbox.agentWork.interrupt(claim, "process stopped after persisting result")
            h.outbox.initializeAgentReceipts()
            h.service.start()
            awaitState(h, req, WriteOutcome.AgentState.APPLIED)
            assertEquals(0, calls.get())
            assertEquals(1, h.lockers.getLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(derivedId.rawValue))!!.version)
        } finally { h.close() }
    }
    @Test fun `arbitrary interrupted effect becomes indeterminate and blocks later work until trusted reconciliation`(): Unit = runBlocking {
        val calls = AtomicInteger()
        val agent = object: LockerAgentRegistry {
            override suspend fun processPayload(roomId: RoomId, lockerId: LockerId, locker: Locker): List<LockerAgentRegistry.LockerWrite> { calls.incrementAndGet(); return emptyList() }
        }
        val h = harness(agent); val first = request(3); val second = request(4, 1)
        try {
            h.rpc.postLockerChanges(first); h.rpc.postLockerChanges(second)
            val dead = h.outbox.agentWork.claim(room, first.writeRequestId, "", 1000, 100)!!
            h.outbox.initializeAgentReceipts()
            h.service.start()
            awaitState(h, first, WriteOutcome.AgentState.INDETERMINATE)
            delay(100)
            assertEquals(0, calls.get())
            assertEquals(WriteOutcome.AgentState.PENDING, h.rpc.getWriteOutcome(GetWriteOutcomeRequest(room, second.writeRequestId)).outcome!!.agentState)
            assertFalse(h.service.reconcileWriteOutcome(room, first.writeRequestId, ByteArray(32), failed = true))
            assertTrue(h.service.reconcileWriteOutcome(room, first.writeRequestId, dead.effectKey, failed = true))
            awaitState(h, second, WriteOutcome.AgentState.APPLIED)
            assertEquals(1, calls.get())
        } finally { h.close() }
    }
    @Test fun `cooperative execution deadlines retain stable effect identity across bounded retries`(): Unit = runBlocking {
        val calls = AtomicInteger(); val keys = java.util.concurrent.CopyOnWriteArrayList<ByteArray>()
        val h = harness(durable(calls) { keys.add(it.copyOf()); awaitCancellation() }, timeoutMs = 20, maxAttempts = 2)
        val req = request(13)
        try {
            h.service.start()
            val response = h.rpc.postLockerChanges(req)
            assertTrue(response.agentFailed); assertEquals(2, calls.get())
            assertContentEquals(keys[0], keys[1])
            assertEquals(WriteOutcome.AgentState.FAILED, h.rpc.getWriteOutcome(GetWriteOutcomeRequest(room, req.writeRequestId)).outcome!!.agentState)
        } finally { h.close() }
    }
    @Test fun `arbitrary execution deadline is indeterminate and is never automatically replayed`(): Unit = runBlocking {
        val calls = AtomicInteger()
        val agent = object: LockerAgentRegistry {
            override suspend fun processPayload(roomId: RoomId, lockerId: LockerId, locker: Locker): List<LockerAgentRegistry.LockerWrite> { calls.incrementAndGet(); awaitCancellation() }
        }
        val h = harness(agent, timeoutMs = 20); val req = request(14)
        try {
            h.service.start()
            val response = h.rpc.postLockerChanges(req)
            assertTrue(response.agentIndeterminate)
            delay(100); assertEquals(1, calls.get())
        } finally { h.close() }
    }

    @Test fun `agent outputs are frozen before telemetry or storage can invoke another extension`(): Unit = runBlocking {
        val payload = byteArrayOf(7); val identity = derivedId.rawValue.copyOf()
        val telemetry = object: LockersTelemetry {
            override suspend fun startSpan(operation: TelemetryOperation): TelemetrySpan? = if (operation != TelemetryOperation.AGENT_EXECUTE) null else object: TelemetrySpan {
                override fun finish(outcome: TelemetryOutcome) { payload[0] = 9; identity[0] = 4 }
            }
        }
        val h = harness(durable { listOf(LockerAgentRegistry.LockerWrite(LockerId(identity), Locker { open { encodedPayload = payload } })) }, telemetry = telemetry)
        try {
            h.service.start(); val response = h.rpc.postLockerChanges(request(15))
            assertFalse(response.agentPending); assertFalse(response.agentFailed)
            val stored = h.lockers.getLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(derivedId.rawValue))!!
            assertContentEquals(byteArrayOf(7), Locker.fromByteArray(stored.locker).open!!.encodedPayload)
            assertNull(h.lockers.getLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(byteArrayOf(4))))
        } finally { h.close() }
    }

    @Test fun `blocked room prefixes cannot starve work beyond the first discovery page`(): Unit = runBlocking {
        val calls = AtomicInteger(); val h = harness(durable(calls)); val target = RoomId(byteArrayOf(127))
        val req = request(12).copy(roomId = target, changes = request(12).changes.map { it.copy(roomId = target) })
        try {
            h.rpc.postLockerChanges(req)
            repeat(64) { index ->
                val blocked = RoomId(byteArrayOf(index.toByte()))
                val first = request(10).copy(roomId = blocked, changes = request(10).changes.map { it.copy(roomId = blocked) })
                h.outbox.agentWork.create(blocked, first, PostLockerChangesResponse(agentPending = true), "old-version", 1)
                assertNull(h.outbox.agentWork.claim(blocked, first.writeRequestId, "fixture/v1", System.currentTimeMillis()))
                h.outbox.agentWork.create(blocked, first.copy(writeRequestId = ByteArray(16) { 11 }), PostLockerChangesResponse(agentPending = true), "fixture/v1", 2)
            }
            h.service.start()
            withTimeout(3000) { h.service.writeOutcomeFlow(target, req.writeRequestId).first { it.agentState == WriteOutcome.AgentState.APPLIED } }
            assertEquals(1, calls.get())
        } finally { h.close() }
    }

    @Test fun `caller cancellation leaves accepted execution owned by the service`(): Unit = runBlocking {
        val entered = CompletableDeferred<Unit>(); val release = CompletableDeferred<Unit>(); val calls = AtomicInteger()
        val h = harness(durable(calls) { entered.complete(Unit); release.await(); emptyList() }); val req = request(5)
        try {
            h.outbox.initializeAgentReceipts()
            h.service.start()
            val caller = launch { h.rpc.postLockerChanges(req) }
            withTimeout(1000) { entered.await() }
            caller.cancelAndJoin()
            release.complete(Unit)
            awaitState(h, req, WriteOutcome.AgentState.APPLIED)
            assertEquals(1, calls.get())
        } finally { release.complete(Unit); h.close() }
    }
    @Test fun `derived commit cannot use the ownership proof captured before an external effect`(): Unit = runBlocking {
        val entered = CompletableDeferred<Unit>(); val release = CompletableDeferred<Unit>()
        var epoch = 1L; var local = true
        val ownership = object: RoomOwnership {
            override suspend fun resolve(keyspace: Long, roomId: RoomId): RoomOwner = if (local) RoomOwner.Local(epoch) else RoomOwner.Remote("other", epoch)
        }
        val h = harness(durable { entered.complete(Unit); release.await(); listOf(LockerAgentRegistry.LockerWrite(derivedId, body(9))) }, ownership)
        val req = request(6)
        try {
            h.outbox.initializeAgentReceipts()
            h.service.start()
            val caller = async { h.rpc.postLockerChanges(req) }
            withTimeout(1000) { entered.await() }
            epoch = 2; local = false; release.complete(Unit)
            delay(150)
            assertNull(h.lockers.getLocker(ServerRoomId(room.rawValue), 0, ServerLockerId(derivedId.rawValue)))
            local = true
            awaitState(h, req, WriteOutcome.AgentState.APPLIED)
            caller.await()
        } finally { release.complete(Unit); h.close() }
    }
}
