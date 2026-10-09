package com.latenighthack.lockers.server.claim

import com.latenighthack.ktstore.PostgresJdbcLimits
import kotlinx.coroutines.*
import java.sql.DriverManager
import kotlin.test.*

class ClaimCancellationLimitsPgTest {
    @Test fun cancelledClaimSqlWaitJoinsAndPreservesCancellation(): Unit = runBlocking {
        val base = PgTestGate.urlOrSkip()
        val name = "claim_sql_cancel_${System.nanoTime()}"
        val url = base + (if ('?' in base) "&" else "?") + "ApplicationName=$name"
        val blocker = DriverManager.getConnection(base)
        val key = System.nanoTime()
        blocker.prepareStatement("SELECT pg_advisory_lock(?)").use { it.setLong(1, key); it.execute() }
        val pool = ClaimJdbcPool(url, limits = PostgresJdbcLimits(socketTimeoutSeconds = 3, statementTimeoutMillis = 1000, lockTimeoutMillis = 250))
        val failed = CompletableDeferred<Throwable>()
        val task = launch {
            try { pool.withConnection { c -> c.prepareStatement("SELECT pg_advisory_lock(?)").use { it.setLong(1, key); it.execute() } } }
            catch (failure: Throwable) { failed.complete(failure) }
        }
        try {
            withTimeout(5000) {
                while (true) {
                    val waiting = blocker.prepareStatement("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name = ? AND wait_event_type = 'Lock')").use {
                        it.setString(1, name); it.executeQuery().use { r -> r.next(); r.getBoolean(1) }
                    }
                    if (waiting) break
                    delay(5)
                }
            }
            withTimeout(1200) { task.cancelAndJoin() }
            assertIs<CancellationException>(failed.await())
            assertEquals(1, pool.withConnection { c -> c.createStatement().use { it.executeQuery("SELECT 1").use { r -> r.next(); r.getInt(1) } } })
        } finally { withContext(NonCancellable) {
            blocker.prepareStatement("SELECT pg_advisory_unlock(?)").use { it.setLong(1, key); it.execute() }
            blocker.close(); task.cancelAndJoin(); pool.close()
        } }
    }
}
