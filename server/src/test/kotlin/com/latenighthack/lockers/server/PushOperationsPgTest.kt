package com.latenighthack.lockers.server

import com.latenighthack.ktstore.Database
import com.latenighthack.ktstore.Store
import com.latenighthack.lockers.server.claim.PgTestGate
import com.latenighthack.lockers.server.services.push.v1.PushQueueStoreImpl
import com.latenighthack.lockers.server.services.push.v1.PushQueueStoreImplDefinitionV1
import com.latenighthack.lockers.server.storage.v1.ServerPush
import com.latenighthack.lockers.server.storage.v1.ServerPushId
import com.latenighthack.lockers.server.storage.v1.ServerSessionId
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertEquals

class PushOperationsPgTest {
    @Test fun independentReportersKeepLegacyAgeUnknownAndAgreeOnSharedBacklog() = runBlocking {
        val base = PgTestGate.urlOrSkip()
        val schema = "push_operations_${System.nanoTime()}"
        DriverManager.getConnection(base).use { conn ->
            conn.createStatement().use { it.execute("CREATE SCHEMA $schema") }
        }
        val url = base + (if ('?' in base) "&" else "?") + "currentSchema=$schema"
        val handles = mutableListOf<Database>()
        suspend fun store(): PushQueueStoreImpl {
            val db = ServerStorage.postgres(url).also { handles.add(it); it.open() }
            return PushQueueStoreImpl(db, clock = { 100 })
        }
        try {
            val stores = listOf(store(), store())
            val historical = object : Store<ServerPush>(handles.first(), PushQueueStoreImplDefinitionV1) {
                suspend fun seed() = save(ServerPush(ServerPushId(byteArrayOf(9)), ServerSessionId(byteArrayOf(1)), 1))
            }
            historical.seed()
            coroutineScope {
                (1..8).map { id -> async {
                    stores[id % 2].enqueue(ServerPush(ServerPushId(byteArrayOf(id.toByte())),
                        ServerSessionId(byteArrayOf(1)), 1), false)
                } }.awaitAll()
            }
            val a = stores.first().snapshotsByBackend().getValue(1)
            val b = store().snapshotsByBackend().getValue(1)
            assertEquals(a, b)
            assertEquals(9, a.depth)
            assertEquals(1, a.unknownAge)
            assertEquals(100, a.oldestAt)
            assertEquals(9, a.eligibleDepth)
        } finally {
            handles.asReversed().forEach { it.close() }
            DriverManager.getConnection(base).use { conn ->
                conn.createStatement().use { it.execute("DROP SCHEMA $schema CASCADE") }
            }
        }
    }
}
