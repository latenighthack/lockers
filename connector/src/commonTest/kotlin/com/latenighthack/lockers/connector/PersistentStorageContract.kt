package com.latenighthack.lockers.connector

import com.latenighthack.ktstore.Database
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.internal.LockerStoreImpl
import com.latenighthack.lockers.connector.storage.v1.StoredLocker
import kotlin.test.*

/** Shared contract executed against each platform's real persistent driver. */
suspend fun verifyPersistentConnectorStorage(factory: () -> Database) {
    val room = RoomId(byteArrayOf(1))
    val space = LockerKeyspace(4)
    val id = LockerId(byteArrayOf(2), space)
    var db = factory()
    db.open()
    try {
        LockerStoreImpl(db).saveLocker(StoredLocker(roomIdRawValue = room.rawValue,
            lockerIdRawValue = id.rawValue, lockerKeyspace = space.value,
            lockerPayload = byteArrayOf(3), version = 11))
    } finally { db.close() }
    db = factory()
    db.open()
    try {
        val store = LockerStoreImpl(db)
        assertEquals(11, store.getAllLockers(room, space).single().version)
        assertContentEquals(byteArrayOf(3), store.getLocker(room, space, id)?.lockerPayload)
        store.deleteLocker(room, space, id)
        assertNull(store.getLocker(room, space, id))
    } finally { db.close() }
}
