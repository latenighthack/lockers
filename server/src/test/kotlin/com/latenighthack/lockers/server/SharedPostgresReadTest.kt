package com.latenighthack.lockers.server

import com.latenighthack.ktstore.Database
import com.latenighthack.ktstore.IndexName
import com.latenighthack.ktstore.Store
import com.latenighthack.ktstore.StoreDefinition
import com.latenighthack.ktstore.StoreName
import com.latenighthack.lockers.server.claim.PgTestGate
import kotlinx.coroutines.runBlocking
import java.sql.DriverManager
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertNull

private val ByteArray.probeIdKey: ByteArray get() = copyOfRange(0, 1)

/** Other server processes write the same Postgres tables, so reads must not come from a process-local row cache. */
class SharedPostgresReadTest {
    private object ProbeDefinition : StoreDefinition<ByteArray>(
        StoreName("shared_read_probe"), "probe-bytes-v1", { it.copyOf() }, { it.copyOf() },
    ) {
        val idKey = bytesIndex(IndexName("id"), ByteArray::probeIdKey, "first-byte-v1").also { primaryKey(it) }
    }
    private class ProbeStore(database: Database) : Store<ByteArray>(database, ProbeDefinition) {
        suspend fun put(value: ByteArray) = save(value)
        suspend fun find(id: Byte) = get(ProbeDefinition.idKey.eq(byteArrayOf(id)))
    }

    private fun execute(url: String, statement: String) =
        DriverManager.getConnection(url).use { it.createStatement().use { sql -> sql.execute(statement) } }

    @Test fun readsObserveRowsChangedOutsideThisProcess() = runBlocking {
        val base = PgTestGate.urlOrSkip()
        val schema = "shared_read_${System.nanoTime()}"
        execute(base, "CREATE SCHEMA $schema")
        val url = base + (if ('?' in base) "&" else "?") + "currentSchema=$schema"
        val database = ServerStorage.postgres(url, additional = listOf(ProbeDefinition))
        database.open()
        try {
            val store = ProbeStore(database)
            store.put(byteArrayOf(1, 7))
            assertContentEquals(byteArrayOf(1, 7), store.find(1))
            execute(url, "DELETE FROM shared_read_probe")
            assertNull(store.find(1))
        } finally {
            database.close()
            execute(base, "DROP SCHEMA $schema CASCADE")
        }
    }
}
