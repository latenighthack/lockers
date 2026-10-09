package com.latenighthack.lockers.server.claim

import com.latenighthack.lockers.common.v1.RoomId
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.*
import java.lang.reflect.Proxy
import java.sql.*
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Logger
import kotlin.test.*

class ClaimResourceDrainTest {
    @Test fun `pool closes connections after an unallocated null slot`(): Unit = runBlocking {
        val url = PgTestGate.urlOrSkip()
        var connection: Connection? = null
        val pool = ClaimJdbcPool(url, 2)
        try {
            pool.withConnection { connection = it }
            pool.close()
            assertTrue(connection!!.isClosed, "null slot must not terminate the close scan")
            assertFails { pool.withConnection {} }
        } finally { pool.close() }
    }

    @Test fun `graceful claim drain joins the renewing coroutine before releasing rows`(): Unit = runBlocking {
        val entered = CompletableDeferred<Unit>()
        val cleanup = CompletableDeferred<Unit>()
        val mayFinish = CompletableDeferred<Unit>()
        val delegate = InMemoryRoomClaimStore()
        val store = object : RoomClaimStore by delegate {
            override suspend fun renewAll(nodeId: String, ttlMs: Long): Set<RoomId> {
                entered.complete(Unit)
                try { awaitCancellation() }
                finally { withContext(NonCancellable) { cleanup.complete(Unit); mayFinish.await() } }
            }
        }
        val meters = ClaimMetrics(SimpleMeterRegistry())
        val ownership = ClaimRoomOwnership(store, "node", "addr", 1000, 10, meters)
        val renewal = ClaimRenewalService(store, ownership, "node", 1000, 10, meters)
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            renewal.start(scope)
            withTimeout(1000) { entered.await() }
            val drained = async(Dispatchers.Default) { renewal.stopAndRelease() }
            withTimeout(1000) { cleanup.await() }
            delay(50)
            assertFalse(drained.isCompleted, "release must await the in-flight renewal cleanup")
            mayFinish.complete(Unit)
            drained.await()
        } finally { mayFinish.complete(Unit); scope.cancel() }
    }
}
