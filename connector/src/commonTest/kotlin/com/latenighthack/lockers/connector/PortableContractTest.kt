package com.latenighthack.lockers.connector

import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.internal.LockerStoreImpl
import com.latenighthack.lockers.connector.storage.v1.StoredLocker
import kotlinx.coroutines.test.runTest
import kotlin.test.*

class PortableContractTest {
    @Test fun scopedCodecCompositionUnwrapsInReverseAndStopsAtDrop() = runTest {
        fun marker(value: Byte) = object : NotificationCodec {
            override suspend fun encode(context: NotificationContext, payload: ByteArray) = payload + value
            override suspend fun decode(context: NotificationContext, payload: ByteArray): ByteArray? =
                if (payload.lastOrNull() == value) payload.dropLast(1).toByteArray() else null
        }
        val space = LockerKeyspace(7)
        val context = NotificationContext(RoomId(byteArrayOf(1)), LockerId(byteArrayOf(2), space), space, "title", "body")
        val chain = NotificationCodecs.builder().add(marker(1)).add(space, marker(2)).add(space, marker(3)).build()
        val original = byteArrayOf(4)
        val encoded = chain.encode(context, original)
        assertContentEquals(byteArrayOf(4, 2, 3), encoded)
        assertContentEquals(original, chain.decode(context, encoded))
        assertNull(chain.decode(context, byteArrayOf(4, 2, 8)))
    }

    @Test fun composedStorageIndexesRoundTripOnEveryTarget() = runTest {
        val database = ConnectorStorage.inMemory()
        database.open()
        try {
            val store = LockerStoreImpl(database)
            val room = RoomId(byteArrayOf(1))
            val space = LockerKeyspace(3)
            val id = LockerId(byteArrayOf(2), space)
            val row = StoredLocker(roomIdRawValue = room.rawValue, lockerIdRawValue = id.rawValue,
                lockerKeyspace = space.value, lockerPayload = byteArrayOf(7), version = 9)
            store.saveLocker(row)
            assertEquals(9, store.getAllLockers(room, space).single().version)
            assertContentEquals(byteArrayOf(7), store.getLocker(room, space, id)?.lockerPayload)
            store.deleteLocker(room, space, id)
            assertTrue(store.getAllLockers(room, space).isEmpty())
        } finally { database.close() }
    }
}
