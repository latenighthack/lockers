package com.latenighthack.lockers.server

import com.latenighthack.ktstore.Database
import com.latenighthack.ktstore.DatabaseConfiguration
import com.latenighthack.ktstore.InMemoryStoreDelegate
import com.latenighthack.ktstore.JdbcDriver
import com.latenighthack.ktstore.LifecycleStoreDelegate
import com.latenighthack.ktstore.ReadCacheConfiguration
import com.latenighthack.ktstore.SqlStoreDelegate
import com.latenighthack.lockers.common.v1.Locker
import com.latenighthack.lockers.common.v1.LockerId
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.room.v1.GetWriteOutcomeRequest
import com.latenighthack.lockers.room.v1.LocalRoomServiceRpc
import com.latenighthack.lockers.room.v1.PostLockerChangeRequest
import com.latenighthack.lockers.room.v1.PostLockerChangesRequest
import com.latenighthack.lockers.room.v1.PostLockerChangesResponse
import com.latenighthack.lockers.room.v1.WriteOutcome
import com.latenighthack.lockers.server.agents.IdempotentLockerAgentRegistry
import com.latenighthack.lockers.server.agents.LockerAgentRegistry
import com.latenighthack.lockers.server.claim.PgTestGate
import com.latenighthack.lockers.server.services.room.v1.DeliveryOutboxStore
import com.latenighthack.lockers.server.services.room.v1.FencedMemoryDelegate
import com.latenighthack.lockers.server.services.room.v1.FencedSqlDriver
import com.latenighthack.lockers.server.services.room.v1.LocalRoomOwnership
import com.latenighthack.lockers.server.services.room.v1.LockStoreImpl
import com.latenighthack.lockers.server.services.room.v1.LockerStoreImpl
import com.latenighthack.lockers.server.services.room.v1.RoomServiceImpl
import com.latenighthack.lockers.server.services.room.v1.SubscriptionStoreImpl
import com.latenighthack.lockers.server.services.session.v1.SessionGatewayDiscovery
import com.latenighthack.lockers.session.v1.SessionGatewayService
import com.latenighthack.lockers.server.storage.v1.ServerRoomId
import com.latenighthack.lockers.server.storage.v1.ServerSessionId
import com.latenighthack.lockers.server.tools.CountingSqlDriver
import com.latenighthack.lockers.server.tools.CountingStoreDelegate
import com.latenighthack.lockers.server.tools.MeasuredStoreDelegate
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Storage cost of one game move: a source write that enqueues agent work, plus the agent's
 * derived frames. Counts use the same notion of a "db op" as [MeasuredStoreDelegate]: request
 * calls run under the write's trace, agent calls run in the background workflow.
 */
class MoveStoreCostTest {
    private val room = RoomId(byteArrayOf(9, 9))
    private val sourceId = LockerId(byteArrayOf(1))
    private val frames = listOf(LockerId(byteArrayOf(2)), LockerId(byteArrayOf(3)))
    private fun body(n: Int) = Locker { open { encodedPayload = byteArrayOf(n.toByte()) } }
    private fun move(n: Int, parent: Long) = PostLockerChangesRequest(roomId = room,
        writeRequestId = ByteArray(16) { n.toByte() },
        changes = listOf(PostLockerChangeRequest(roomId = room, lockerId = sourceId, locker = body(n),
            parentVersion = parent)))
    private val agent = object : IdempotentLockerAgentRegistry {
        override val agentVersion = "move-cost/v1"
        override suspend fun processPayload(effectKey: ByteArray, roomId: RoomId, lockerId: LockerId, locker: Locker) =
            frames.map { LockerAgentRegistry.LockerWrite(it, locker) }
    }
    private val noGateway = object : SessionGatewayDiscovery {
        override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = null
    }

    /** Receives advisory lock statements counted below the fenced SQL driver. */
    private var locks: CountingStoreDelegate? = null

    private class Harness(val db: Database, val counter: CountingStoreDelegate, val outbox: DeliveryOutboxStore,
        val service: RoomServiceImpl) {
        val rpc = LocalRoomServiceRpc(service)
    }

    private suspend fun harness(configuration: DatabaseConfiguration, delegate: LifecycleStoreDelegate,
        countLockRequests: Boolean = true): Harness {
        val counter = CountingStoreDelegate(delegate, countLockRequests).also { locks = it }
        val db = Database(configuration, counter, ReadCacheConfiguration.Disabled)
        val outbox = DeliveryOutboxStore(db).also { it.prepareStores() }
        val lockers = LockerStoreImpl(db).also { it.prepare() }
        val locks = LockStoreImpl(db).also { it.prepare() }
        val subs = SubscriptionStoreImpl(db).also { it.prepare() }
        db.open()
        repeat(2) { subs.addSubscription(ServerSessionId(byteArrayOf(7, it.toByte())), ServerRoomId(room.rawValue)) }
        val config = LockersConfig.defaults().copy(deliveryWorkerEnabled = false)
        val service = RoomServiceImpl(subs, lockers, locks, noGateway, LocalRoomOwnership(), agent,
            SimpleMeterRegistry(), config, outbox)
        return Harness(db, counter, outbox, service)
    }

    private suspend fun measure(h: Harness, lockBudget: Int? = null) {
        h.service.start()
        try {
            // First move admits new lockers; the measured move is the steady state of a game.
            val first = h.rpc.postLockerChanges(move(1, 0))
            assertFalse(first.agentPending, "first move should observe its applied frames")
            delay(SETTLE_MS)
            h.counter.reset()
            val response = h.rpc.postLockerChanges(move(2, 1))
            val move = h.counter.snapshot()
            assertEquals(PostLockerChangesResponse.Result.OK, response.result)
            assertFalse(response.agentPending, "request should observe its applied frames")
            println("MOVE COST\n$move")
            // The receipt is not rewritten when frames apply; replays and outcomes read the journal.
            val replay = h.rpc.postLockerChanges(move(2, 1))
            assertEquals(PostLockerChangesResponse.Result.OK, replay.result)
            val settled = !replay.agentPending && !replay.agentFailed && !replay.agentIndeterminate
            assertTrue(settled, "replay of an applied move: $replay")
            val outcome = h.rpc.getWriteOutcome(GetWriteOutcomeRequest(room, move(2, 1).writeRequestId)).outcome
            assertEquals(WriteOutcome.AgentState.APPLIED, outcome?.agentState)
            delay(SETTLE_MS)
            h.counter.reset()
            delay(IDLE_WINDOW_MS)
            val idle = h.counter.snapshot()
            println("IDLE 3s\n$idle")
            // Admission reads counters; no COUNT scan remains on either side of a move.
            val keys = move.request.keys + move.background.keys
            assertTrue(keys.none { it.startsWith("count:") }, "COUNT on the move path: $move")
            assertTrue(move.requestOps <= REQUEST_BUDGET, "request side: ${move.requestOps} > $REQUEST_BUDGET\n$move")
            assertTrue(move.backgroundOps <= AGENT_BUDGET, "agent side: ${move.backgroundOps} > $AGENT_BUDGET\n$move")
            // A logical lock the transaction already holds is not requested from Postgres again.
            lockBudget?.let { assertTrue(move.requestLocks <= it, "advisory lock statements: $move") }
            // Discovery is woken in-process; an idle room service only runs its slow safety-net scan.
            assertTrue(idle.backgroundOps <= IDLE_ROOM_BUDGET, "idle room service storage calls in 3s: $idle")
        } finally { h.service.closeAndJoin(); h.db.close() }
    }

    private companion object {
        const val REQUEST_BUDGET = 16
        const val AGENT_BUDGET = 18
        const val REQUEST_LOCK_BUDGET = 4
        const val IDLE_ROOM_BUDGET = 4
        const val IDLE_POLL_BUDGET = 3
        const val SETTLE_MS = 300L
        const val IDLE_WINDOW_MS = 3_000L
        const val WARMUP_MS = 5_000L
        const val SERVER_WINDOW_MS = 5_000L
        val externalTables = setOf("room_claim", "room_claim_capacity", "session_gateway", "shard_map", "shard_fence")
    }

    @Test fun `in-memory move and idle storage cost`(): Unit = runBlocking {
        val configuration = ServerStorage.configuration("move-cost-${kotlin.random.Random.nextLong()}")
        measure(harness(configuration, MeasuredStoreDelegate(FencedMemoryDelegate(InMemoryStoreDelegate()))))
    }

    @Test fun `room blocked behind indeterminate work keeps discovery backed off`(): Unit = runBlocking {
        val configuration = ServerStorage.configuration("blocked-${kotlin.random.Random.nextLong()}")
        val h = harness(configuration, MeasuredStoreDelegate(FencedMemoryDelegate(InMemoryStoreDelegate())))
        // Work journaled by a retired agent version cannot run here: the first item becomes
        // indeterminate and the second stays due behind it until trusted reconciliation.
        val pending = PostLockerChangesResponse(agentPending = true)
        h.outbox.agentWork.create(room, move(1, 0), pending, "retired/v0", 1)
        h.outbox.agentWork.create(room, move(2, 1), pending, "retired/v0", 2)
        h.service.start()
        try {
            delay(WARMUP_MS)
            h.counter.reset()
            delay(SERVER_WINDOW_MS)
            val blocked = h.counter.snapshot()
            println("BLOCKED ROOM 5s\n$blocked")
            // Each safety-net scan costs a discovery read and one claim attempt for the blocked room.
            assertTrue(blocked.backgroundOps <= 2 * IDLE_POLL_BUDGET, "blocked room discovery: $blocked")
        } finally { h.service.closeAndJoin(); h.db.close() }
    }

    @Test fun `full server idle storage cost`(): Unit = runBlocking {
        val counter = CountingStoreDelegate(MeasuredStoreDelegate(FencedMemoryDelegate(InMemoryStoreDelegate())))
        val configuration = ServerStorage.configuration("idle-${kotlin.random.Random.nextLong()}")
        val database = Database(configuration, counter, ReadCacheConfiguration.Disabled)
        val core = ServerCore::class.create(LockersConfig.defaults(), database)
        core.overridePushProviders = emptyList()
        core.setup()
        val component = MonolithComponent(core)
        try {
            component.start()
            delay(WARMUP_MS)
            counter.reset()
            delay(SERVER_WINDOW_MS)
            val idle = counter.snapshot()
            println("FULL SERVER IDLE 5s\n$idle")
            // Work queues wait on in-process signals; their fallback polls back off to seconds.
            // The fixed five-second operations snapshot is observability, not a work poll.
            fun calls(key: String) = idle.background[key] ?: 0
            assertTrue(calls("query:agent_work_v2/due_ordered") <= IDLE_POLL_BUDGET, "agent discovery: $idle")
            assertTrue(idle.background.keys.none { it.startsWith("deleteBatch:") }, "retention pruning: $idle")
            assertTrue(calls("query:delivery_outbox_heads_v2/due_time") <= IDLE_POLL_BUDGET, "room outbox polls: $idle")
            val pushOutboxPolls = calls("query:push_delivery_outbox_heads_v2/due_time")
            assertTrue(pushOutboxPolls <= IDLE_POLL_BUDGET, "push outbox polls: $idle")
            assertTrue(calls("query:push_work_v2/backend_due_ordered") <= IDLE_POLL_BUDGET, "push claim polls: $idle")
        } finally { component.stop() }
    }

    @Test fun `postgres move and idle storage cost`(): Unit = runBlocking {
        val base = PgTestGate.urlOrSkip(); val schema = "move_cost_${System.nanoTime()}"
        DriverManager.getConnection(base).use { it.createStatement().use { s -> s.execute("CREATE SCHEMA $schema") } }
        try {
            val url = base + (if ('?' in base) "&" else "?") + "currentSchema=$schema"
            val configuration = ServerStorage.configuration("lockers-server").copy(externalTables = externalTables)
            // Count advisory lock statements actually sent, after re-entrant locks are elided.
            val jdbc = CountingSqlDriver(JdbcDriver(url.removePrefix("jdbc:postgresql:"), "postgresql")) {
                requireNotNull(locks).recordLock()
            }
            val delegate = MeasuredStoreDelegate(SqlStoreDelegate(FencedSqlDriver(jdbc), "BYTEA", configuration))
            measure(harness(configuration, delegate, countLockRequests = false), lockBudget = REQUEST_LOCK_BUDGET)
        } finally {
            DriverManager.getConnection(base).use { connection ->
                connection.createStatement().use { it.execute("DROP SCHEMA $schema CASCADE") }
            }
        }
    }
}
