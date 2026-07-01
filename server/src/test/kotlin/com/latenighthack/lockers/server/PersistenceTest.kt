package com.latenighthack.lockers.server

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import com.latenighthack.ktstore.JdbcDriver
import com.latenighthack.ktstore.SqlStoreDelegate
import com.latenighthack.lockers.server.services.room.v1.LockerStoreImpl
import com.latenighthack.lockers.server.storage.v1.ServerLocker
import com.latenighthack.lockers.server.storage.v1.ServerLockerId
import com.latenighthack.lockers.server.storage.v1.ServerRoomId
import kotlinx.coroutines.test.runTest
import java.io.File
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * Proves the SQL-backed StoreDelegate persists app data across a full "restart"
 * (a brand-new delegate + store instance over the same database file).
 *
 * Production uses Postgres via ktstore's createStoreDelegate, which is
 * SqlStoreDelegate(JdbcDriver(db, "postgresql"), blobType = "BYTEA"). No Postgres
 * server is available in CI, so this exercises the same SqlStoreDelegate code
 * path against on-disk SQLite — enough to prove durable round-tripping through
 * the real store the server uses.
 */
class PersistenceTest {
    @Test
    fun lockerSurvivesRestart() = runTest {
        val dbFile = File.createTempFile("lockers-persistence", ".db").also {
            it.delete()
            it.deleteOnExit()
        }
        val jdbcDb = dbFile.absolutePath

        val roomId = ServerRoomId(byteArrayOf(1, 2, 3))
        val lockerId = ServerLockerId(byteArrayOf(9, 8, 7))
        val keyspace = 42L
        val payload = byteArrayOf(0x10, 0x20, 0x30, 0x40)

        // First boot: create schema, write a locker, then discard the delegate.
        run {
            val delegate = SqlStoreDelegate(JdbcDriver(jdbcDb, "sqlite"), blobType = "BLOB")
            val store = LockerStoreImpl(delegate)
            store.prepare()
            delegate.createStores()
            store.updateLocker(
                ServerLocker {
                    this.roomId = roomId
                    this.lockerId = lockerId
                    this.keyspace = keyspace
                    this.locker = payload
                    this.version = 1L
                }
            )
        }

        // Second boot: a fresh delegate + store over the SAME file must see the locker.
        val reloaded = run {
            val delegate = SqlStoreDelegate(JdbcDriver(jdbcDb, "sqlite"), blobType = "BLOB")
            val store = LockerStoreImpl(delegate)
            store.prepare()
            delegate.createStores()
            store.getLocker(roomId, keyspace, lockerId)
        }

        assertThat(reloaded).isNotNull()
        assertThat(reloaded!!.version).isEqualTo(1L)
        assertTrue(reloaded.locker.contentEquals(payload))
    }
}
