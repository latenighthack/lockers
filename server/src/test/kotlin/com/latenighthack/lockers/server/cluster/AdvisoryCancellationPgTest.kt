package com.latenighthack.lockers.server.cluster

import com.latenighthack.lockers.server.claim.PgTestGate
import kotlinx.coroutines.*
import java.sql.DriverManager
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.coroutines.CoroutineContext
import kotlin.test.*

class AdvisoryCancellationPgTest {
    private class QueuedDispatcher : CoroutineDispatcher() {
        private val pending = ConcurrentLinkedQueue<Runnable>()
        override fun dispatch(context: CoroutineContext, block: Runnable) { pending.add(block) }
        fun runOne() { pending.poll()?.run() }
        val hasPending get() = pending.isNotEmpty()
    }

    @Test fun cancellationDuringLeaseHandoffReleasesTheActualPostgresLock(): Unit = runBlocking {
        val base = PgTestGate.urlOrSkip()
        val schema = "lease_cancel_${System.nanoTime()}"
        DriverManager.getConnection(base).use { it.createStatement().use { sql -> sql.execute("CREATE SCHEMA $schema") } }
        val url = base + (if ('?' in base) "&" else "?") + "currentSchema=$schema&ApplicationName=$schema"
        val dispatcher = QueuedDispatcher()
        val gateway = JdbcAdvisoryLockGateway(url)
        val key = System.nanoTime()
        val task = launch(dispatcher) {
            gateway.tryLock(key)?.close()
            fail("The canceled caller must never receive the lease")
        }
        try {
            dispatcher.runOne()
            // The IO block has allocated a lease; its handoff is queued on the caller dispatcher.
            withTimeout(5000) { while (!dispatcher.hasPending) delay(1) }
            task.cancel()
            withTimeout(5000) { while (!task.isCompleted) { dispatcher.runOne(); delay(1) } }
            val replacement = assertNotNull(gateway.tryLock(key), "A canceled handoff must release its acquired PostgreSQL advisory lock")
            replacement.close()
        } finally {
            task.cancel()
            withTimeout(5000) { while (!task.isCompleted) { dispatcher.runOne(); delay(1) } }
            DriverManager.getConnection(base).use { connection ->
                connection.prepareStatement("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = ? AND pid <> pg_backend_pid()").use { sql ->
                    sql.setString(1, schema); sql.execute()
                }
                connection.createStatement().use { it.execute("DROP SCHEMA $schema CASCADE") }
            }
        }
    }
}
