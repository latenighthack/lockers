package com.latenighthack.lockers.connector

import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.internal.LockerStoreImpl
import com.latenighthack.lockers.connector.storage.v1.StoredLocker
import kotlinx.coroutines.test.runTest
import kotlin.test.*

class LockerStoreIsolationTest {
    @Test fun savedAndAcceptedRowsDoNotRetainCallerArrays() = runTest {
        val database = ConnectorStorage.inMemory()
        database.open()
        try {
            val store = LockerStoreImpl(database)
            for (accept in listOf(false, true)) {
                val row = row()
                if (accept) store.acceptLocker(row) else store.saveLocker(row)
                poison(row)
                assertOriginal(requireNotNull(store.getLocker(room, space, id)))
            }
        } finally { database.close() }
    }

    @Test fun everyPublicReadDetachesStoredRows() = runTest {
        val database = ConnectorStorage.inMemory()
        database.open()
        try {
            val store = LockerStoreImpl(database)
            store.saveLocker(row())
            val reads: List<suspend () -> StoredLocker> = listOf(
                { requireNotNull(store.getLocker(room, space, id)) },
                { store.getAllLockers().single() },
                { store.getAllLockers(room).single() },
                { store.getAllLockers(room, space).single() },
            )
            for (read in reads) {
                poison(read())
                assertOriginal(requireNotNull(store.getLocker(room, space, id)))
            }
        } finally { database.close() }
    }

    private fun row() = StoredLocker(roomIdRawValue = byteArrayOf(1), lockerIdRawValue = byteArrayOf(2),
        lockerKeyspace = 3, lockerPayload = byteArrayOf(7), version = 9)
    private fun poison(row: StoredLocker) {
        row.roomIdRawValue[0] = 99; row.lockerIdRawValue[0] = 98; row.lockerPayload[0] = 6
    }
    private fun assertOriginal(row: StoredLocker) {
        assertContentEquals(byteArrayOf(1), row.roomIdRawValue)
        assertContentEquals(byteArrayOf(2), row.lockerIdRawValue)
        assertContentEquals(byteArrayOf(7), row.lockerPayload)
        assertEquals(9, row.version)
    }
    private val room = RoomId(byteArrayOf(1))
    private val space = LockerKeyspace(3)
    private val id = LockerId(byteArrayOf(2), space)
}
