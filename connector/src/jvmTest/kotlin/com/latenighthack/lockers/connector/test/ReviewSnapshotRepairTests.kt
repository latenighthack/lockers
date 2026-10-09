package com.latenighthack.lockers.connector.test

import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.connector.internal.*
import com.latenighthack.lockers.connector.storage.v1.StoredLocker
import com.latenighthack.lockers.room.v1.*
import kotlinx.coroutines.*
import kotlin.test.*

class ReviewSnapshotRepairTests {
    @Test fun `an authoritative missing read removes unchanged cached content`() = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        val room = RoomId(byteArrayOf(1)); val id = LockerId(byteArrayOf(2), LockerKeyspace(0))
        LockerStoreImpl(db).saveLocker(StoredLocker(roomIdRawValue = room.rawValue, lockerIdRawValue = id.rawValue,
            lockerKeyspace = 0, version = 5, lockerPayload = byteArrayOf(3)))
        val client = reviewClient(ReviewRpc { method, _ -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse().toByteArray()
            "GetLocker" -> GetLockerResponse().toByteArray()
            else -> error(method.methodName)
        } }, db = db)
        try {
            assertNull(client.getLockers(room, listOf(id)).single())
            assertNull(client.getLocker(room, id, revalidate = false), "A missing server record must repair the cached record")
        } finally { client.closeAndJoin() }
    }

    @Test fun `a paged snapshot drains all pages before publishing the complete set`() = runBlocking {
        val room = RoomId(byteArrayOf(1)); val keyspace = LockerKeyspace(0)
        val requests = mutableListOf<GetAllLockersRequest>()
        fun value(id: Int) = IdentifiedLocker(LockerId(byteArrayOf(id.toByte()), keyspace), com.latenighthack.lockers.common.v1.Locker(open = com.latenighthack.lockers.common.v1.Locker.OpenLocker(encodedPayload = byteArrayOf(id.toByte()))), version = 1)
        val client = reviewClient(ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(snapshotPaging = true).toByteArray()
            "GetAllLockers" -> {
                val request = GetAllLockersRequest.fromByteArray(bytes); requests += request
                if (request.pageToken.isEmpty()) GetAllLockersResponse(lockers = listOf(value(2)), nextPageToken = byteArrayOf(7), roomSequence = 42).toByteArray()
                else GetAllLockersResponse(lockers = listOf(value(3)), roomSequence = 42).toByteArray()
            }
            else -> error(method.methodName)
        } })
        try {
            assertEquals(2, client.getAllLockers(room, keyspace).size)
            assertEquals(2, requests.size)
            assertTrue(requests.all { it.pageSize == 64 })
        } finally { client.closeAndJoin() }
    }
    @Test fun `missing read cannot erase cache accepted while the request was in flight`() = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        val room = RoomId(byteArrayOf(1)); val id = LockerId(byteArrayOf(2), LockerKeyspace(0))
        val store = LockerStoreImpl(db)
        val old = StoredLocker(roomIdRawValue = room.rawValue, lockerIdRawValue = id.rawValue, version = 5, lockerPayload = byteArrayOf(3))
        store.saveLocker(old)
        val entered = CompletableDeferred<Unit>(); val release = CompletableDeferred<Unit>()
        val client = reviewClient(ReviewRpc { method, _ -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse().toByteArray()
            "GetLocker" -> { entered.complete(Unit); release.await(); GetLockerResponse().toByteArray() }
            else -> error(method.methodName)
        } }, db = db)
        try {
            val read = async { client.getLockers(room, listOf(id)) }
            entered.await(); store.saveLocker(old.copy(version = 6, lockerPayload = byteArrayOf(4)))
            release.complete(Unit); read.await()
            assertEquals(6, client.getLocker(room, id, revalidate = false)!!.version)
        } finally { client.closeAndJoin() }
    }

    @Test fun `inconsistent snapshot watermark rolls back all pages`() = runBlocking {
        val db = ConnectorStorage.inMemory()
        val client = reviewClient(ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(snapshotPaging = true).toByteArray()
            "GetAllLockers" -> if (GetAllLockersRequest.fromByteArray(bytes).pageToken.isEmpty())
                GetAllLockersResponse(nextPageToken = byteArrayOf(7), roomSequence = 42).toByteArray()
                else GetAllLockersResponse(roomSequence = 43).toByteArray()
            else -> error(method.methodName)
        } }, db = db)
        try {
            assertFailsWith<IllegalArgumentException> { client.getAllLockers(RoomId(byteArrayOf(1)), LockerKeyspace(0)) }
            assertTrue(LockerStoreImpl(db).getAllLockers().isEmpty())
        } finally { client.closeAndJoin() }
    }
}
