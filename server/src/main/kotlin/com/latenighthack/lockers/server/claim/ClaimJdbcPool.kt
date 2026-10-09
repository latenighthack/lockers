package com.latenighthack.lockers.server.claim

import com.latenighthack.ktstore.PostgresJdbcLimits
import com.latenighthack.lockers.server.tools.safeMeters
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import kotlinx.coroutines.channels.Channel
import java.sql.Connection
import java.util.Collections
import java.util.IdentityHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.coroutineContext

/** Bounded coordination pool. Operations reporting uses its own pool and cannot starve renewals. */
class ClaimJdbcPool @JvmOverloads constructor(
    private val jdbcUrl: String,
    private val size: Int = 2,
    private val limits: PostgresJdbcLimits = PostgresJdbcLimits(),
    private val registry: MeterRegistry? = null,
    private val purpose: Purpose = Purpose.CLAIM,
) : AutoCloseable {
    enum class Purpose(val tag: String) { CLAIM("claim"), OPERATIONS("operations") }
    private val slots = Channel<Connection?>(size.also { require(it > 0) { "ClaimJdbcPool size must be positive" } })
    private val closed = AtomicBoolean(false)
    private val active = AtomicInteger()
    private val waiting = AtomicInteger()
    private val allocated = AtomicInteger()
    private val connections = Collections.synchronizedMap(IdentityHashMap<Connection, Boolean>())

    init {
        registry?.safeMeters {
            for ((state, value) in listOf("active" to active, "waiting" to waiting, "total" to allocated)) {
                gauge("fullhouse.database.connections", Tags.of("pool", purpose.tag, "state", state), value) {
                    it.get().toDouble() }
            }
            Gauge.builder("fullhouse.database.connections") { maxOf(0, allocated.get() - active.get()).toDouble() }
                .tags("pool", purpose.tag, "state", "idle").register(this)
            Gauge.builder("fullhouse.database.connections") { size.toDouble() }
                .tags("pool", purpose.tag, "state", "capacity").register(this)
        }
        repeat(size) { check(slots.trySend(null).isSuccess) }
    }

    private fun record(stage: String, started: Long, outcome: String) {
        registry?.safeMeters { timer("fullhouse.database.duration", "pool", purpose.tag, "stage", stage,
            "outcome", outcome)
            .record(System.nanoTime() - started, TimeUnit.NANOSECONDS) }
    }
    private fun outcome(error: Throwable) = when (error) {
        is TimeoutCancellationException -> "timeout"
        is CancellationException -> "cancelled"
        else -> "error"
    }
    private fun retire(connection: Connection?) {
        if (connection == null) return
        if (connections.remove(connection) != null) allocated.decrementAndGet()
        runCatching { connection.close() }
    }

    // Every failure returns the slot and closes its connection before propagating.
    @Suppress("TooGenericExceptionCaught")
    suspend fun <T> withConnection(block: (Connection) -> T): T {
        check(!closed.get()) { "ClaimJdbcPool is closed" }
        val queued = System.nanoTime()
        waiting.incrementAndGet()
        var acquisitionOutcome = "success"
        var conn = try { slots.receive() } catch (error: Throwable) {
            acquisitionOutcome = outcome(error)
            throw error
        } finally { waiting.decrementAndGet(); record("acquire", queued, acquisitionOutcome) }
        active.incrementAndGet()
        val dispatched = System.nanoTime()
        var executionStarted: Long? = null
        var resultOutcome = "error"
        try {
            val result = withContext(Dispatchers.IO) {
                record("dispatch", dispatched, "success")
                check(!closed.get()) { "ClaimJdbcPool is closed" }
                if (conn == null || !isUsable(conn)) {
                    retire(conn); conn = null
                    val started = System.nanoTime()
                    var connected = false
                    try {
                        conn = com.latenighthack.lockers.server.tools.openPostgresConnection(jdbcUrl, limits)
                        connections[conn] = true
                        allocated.incrementAndGet()
                        connected = true
                    } finally { record("connect", started, if (connected) "success" else "error") }
                }
                executionStarted = System.nanoTime()
                block(conn)
            }
            if (closed.get() || slots.trySend(conn).isFailure) retire(conn)
            resultOutcome = "success"
            return result
        } catch (error: Throwable) {
            withContext(NonCancellable + Dispatchers.IO) { retire(conn) }
            slots.trySend(null)
            resultOutcome = outcome(error)
            coroutineContext.ensureActive()
            throw error
        } finally { active.decrementAndGet(); executionStarted?.let { record("execute", it, resultOutcome) } }
    }

    private fun isUsable(conn: Connection) = runCatching { !conn.isClosed && conn.isValid(1) }.getOrDefault(false)
    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        slots.close()
        while (true) {
            val received = slots.tryReceive()
            if (received.isFailure) break
            retire(received.getOrNull())
        }
    }
}
