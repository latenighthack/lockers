package com.latenighthack.lockers.server

import com.latenighthack.ktstore.Database
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.agents.*
import com.latenighthack.lockers.server.claim.PgTestGate
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.server.storage.v2.*
import com.latenighthack.lockers.session.v1.*
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first
import java.sql.DriverManager
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.*

class AgentWorkflowPgTest {
    private val room = RoomId(byteArrayOf(1)); private val id = LockerId(byteArrayOf(2))
    private fun body() = Locker { open { encodedPayload = byteArrayOf(3) } }
    private fun request(n: Int) = PostLockerChangesRequest(roomId = room, writeRequestId = ByteArray(16) { n.toByte() }, changes = listOf(PostLockerChangeRequest(roomId = room, lockerId = id, locker = body())))
    private suspend fun withSchema(block: suspend (String, MutableList<Database>) -> Unit) {
        val base = PgTestGate.urlOrSkip(); val schema = "agent_work_${System.nanoTime()}"
        DriverManager.getConnection(base).use { it.createStatement().use { s -> s.execute("CREATE SCHEMA $schema") } }
        val handles = mutableListOf<Database>()
        try { block(base + (if ('?' in base) "&" else "?") + "currentSchema=$schema", handles) }
        finally {
            handles.forEach { it.close() }
            DriverManager.getConnection(base).use { it.createStatement().use { s -> s.execute("DROP SCHEMA $schema CASCADE") } }
        }
    }
    @Test fun `new database handle recovers committed source input and preserves exact source identities`(): Unit = runBlocking {
        withSchema { url, handles ->
            val calls = AtomicInteger()
            val agent = object: IdempotentLockerAgentRegistry {
                override val agentVersion = "postgres/v1"
                override suspend fun processPayload(effectKey: ByteArray, roomId: RoomId, lockerId: LockerId, locker: Locker): List<LockerAgentRegistry.LockerWrite> {
                    calls.incrementAndGet(); return listOf(LockerAgentRegistry.LockerWrite(LockerId(byteArrayOf(4)), body()))
                }
            }
            suspend fun service(): RoomServiceImpl {
                val db = ServerStorage.postgres(url).also { handles.add(it) }
                val outbox = DeliveryOutboxStore(db).also { it.prepareStores() }
                val lockers = LockerStoreImpl(db).also { it.prepare() }; val locks = LockStoreImpl(db).also { it.prepare() }; val subs = SubscriptionStoreImpl(db).also { it.prepare() }
                db.open()
                return RoomServiceImpl(subs, lockers, locks, object: SessionGatewayDiscovery { override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = null },
                    LocalRoomOwnership(), agent, SimpleMeterRegistry(), LockersConfig.defaults().copy(deliveryWorkerEnabled = false), outbox)
            }
            val first = service(); val req = request(1)
            assertTrue(LocalRoomServiceRpc(first).postLockerChanges(req).agentPending)
            first.closeAndJoin()
            val second = service()
            try {
                second.start()
                val outcome = withTimeout(5000) { second.writeOutcomeFlow(room, req.writeRequestId).first { it.agentState == WriteOutcome.AgentState.APPLIED } }
                assertEquals(id, outcome.sourceVersions.single().lockerId)
                assertEquals(1, outcome.sourceVersions.single().version)
                assertEquals(1, calls.get())
            } finally { second.closeAndJoin() }
        }
    }
    @Test fun `independent handles fence stale agent results and serialize retained capacity`(): Unit = runBlocking {
        withSchema { url, handles ->
            var now = 1L
            suspend fun open(): AgentWorkStore {
                val db = ServerStorage.postgres(url).also { handles.add(it) }
                val store = AgentWorkStore(db, { now }, globalCapacity = 1).also { it.prepare() }
                db.open(); return store
            }
            val a = open(); val b = open(); val req = request(2)
            val start = CompletableDeferred<Unit>()
            val results = listOf(a, b).mapIndexed { n, store -> async(Dispatchers.IO) {
                start.await(); runCatching { store.create(room, request(n + 2), PostLockerChangesResponse(agentPending = true), "fixture/v1", 1) }
            } }
            start.complete(Unit)
            val outcomes = results.awaitAll()
            assertEquals(1, outcomes.count { it.isSuccess })
            assertIs<IllegalStateException>(outcomes.single { it.isFailure }.exceptionOrNull())
            val row = a.candidates(now).first.single()
            val first = a.claim(room, row.writeRequestId, "fixture/v1", now, 10)!!
            now = 11
            val second = b.claim(room, row.writeRequestId, "fixture/v1", now, 10)!!
            assertNull(a.result(first, emptyList()))
            assertNotNull(b.result(second, emptyList()))
        }
    }
}
