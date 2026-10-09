package com.latenighthack.lockers.connector.test
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.connector.internal.*
import com.latenighthack.lockers.connector.storage.v1.StoredLocker
import kotlinx.coroutines.runBlocking
import kotlin.test.*
class ReviewDeleteReceiptTests {
    @Test fun `lost delete response reuses one frozen receipt and notification`() = runBlocking {
        val db = ConnectorStorage.inMemory(); val room = RoomId(byteArrayOf(1)); val id = LockerId(byteArrayOf(2))
        var first: ByteArray? = null; var calls = 0; var encodes = 0
        val notification = byteArrayOf(4)
        val client = reviewClient(ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(writeReceipts = true, deleteReceipts = true).toByteArray()
            "DeleteLocker" -> {
                val request = DeleteLockerRequest.fromByteArray(bytes)
                assertEquals(32, request.writeRequestId.size)
                if (first == null) first = bytes.copyOf() else assertContentEquals(first, bytes)
                if (++calls == 1) { notification.fill(9); throw FaultInjectingRpcClient.rpcError(com.latenighthack.ktbuf.proto.Codes.UNAVAILABLE) }
                DeleteLockerResponse(version = 2, writeRequestId = request.writeRequestId).toByteArray()
            }
            else -> error(method.methodName)
        } }, db = db, codecs = NotificationCodecs.of(object : NotificationCodec {
            override suspend fun decode(context: NotificationContext, payload: ByteArray) = payload
            override suspend fun encode(context: NotificationContext, payload: ByteArray): ByteArray { encodes++; return payload }
        }))
        LockerStoreImpl(db).saveLocker(StoredLocker(roomIdRawValue = room.rawValue, lockerIdRawValue = id.rawValue, version = 1, lockerPayload = byteArrayOf(3)))
        try {
            client.deleteLocker(room, id) { payload { rawValue = notification } }
            assertEquals(2, calls); assertEquals(1, encodes)
            assertNull(client.getLocker(room, id, false))
        } finally { client.closeAndJoin() }
    }
    @Test fun `legacy ambiguity cannot auto rebase a deletion onto recreated content`() = runBlocking {
        val db = ConnectorStorage.inMemory(); val room = RoomId(byteArrayOf(1)); val id = LockerId(byteArrayOf(2))
        var deletes = 0
        val client = reviewClient(ReviewRpc { method, _ -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse().toByteArray()
            "DeleteLocker" -> when (++deletes) {
                1 -> throw FaultInjectingRpcClient.rpcError(com.latenighthack.ktbuf.proto.Codes.UNAVAILABLE)
                2 -> DeleteLockerResponse(result = DeleteLockerResponse.Result.UPDATE_LOCAL_VERSION, version = 3).toByteArray()
                else -> DeleteLockerResponse(version = 4).toByteArray()
            }
            "GetLocker" -> GetLockerResponse(locker = IdentifiedLocker(id, com.latenighthack.lockers.common.v1.Locker(open = com.latenighthack.lockers.common.v1.Locker.OpenLocker(byteArrayOf(9))), 3)).toByteArray()
            else -> error(method.methodName)
        } }, db = db)
        LockerStoreImpl(db).saveLocker(StoredLocker(roomIdRawValue = room.rawValue, lockerIdRawValue = id.rawValue, version = 1, lockerPayload = byteArrayOf(3)))
        try {
            val conflict = assertFailsWith<LockerDeleteConflictException> { client.deleteLocker(room, id) }
            assertEquals(1, conflict.expectedVersion); assertEquals(3, conflict.actualVersion); assertTrue(conflict.mayHaveCommitted)
            assertEquals(2, deletes)
            assertEquals(3, client.getLocker(room, id, false)!!.version)
        } finally { client.closeAndJoin() }
    }
    @Test fun `old receipt replay preserves a newer cached incarnation`() = runBlocking {
        val db = ConnectorStorage.inMemory(); val room = RoomId(byteArrayOf(1)); val id = LockerId(byteArrayOf(2))
        val client = reviewClient(ReviewRpc { method, bytes -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(deleteReceipts = true).toByteArray()
            "DeleteLocker" -> {
                val request = DeleteLockerRequest.fromByteArray(bytes)
                LockerStoreImpl(db).saveLocker(StoredLocker(roomIdRawValue = room.rawValue, lockerIdRawValue = id.rawValue, version = 3, lockerPayload = byteArrayOf(9)))
                DeleteLockerResponse(version = 2, writeRequestId = request.writeRequestId).toByteArray()
            }
            else -> error(method.methodName)
        } }, db = db)
        LockerStoreImpl(db).saveLocker(StoredLocker(roomIdRawValue = room.rawValue, lockerIdRawValue = id.rawValue, version = 1, lockerPayload = byteArrayOf(3)))
        try { client.deleteLocker(room, id); assertEquals(3, client.getLocker(room, id, false)!!.version) }
        finally { client.closeAndJoin() }
    }
    @Test fun `reused receipt rejection is terminal`() = runBlocking {
        val db = ConnectorStorage.inMemory(); val room = RoomId(byteArrayOf(1)); val id = LockerId(byteArrayOf(2)); var calls = 0
        val client = reviewClient(ReviewRpc { method, _ -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(deleteReceipts = true).toByteArray()
            "DeleteLocker" -> { calls++; DeleteLockerResponse(result = DeleteLockerResponse.Result.REQUEST_ID_REUSED).toByteArray() }
            else -> error(method.methodName)
        } }, db = db)
        LockerStoreImpl(db).saveLocker(StoredLocker(roomIdRawValue = room.rawValue, lockerIdRawValue = id.rawValue, version = 1, lockerPayload = byteArrayOf(3)))
        try { assertFailsWith<LockerWriteException> { client.deleteLocker(room, id) }; assertEquals(1, calls) }
        finally { client.closeAndJoin() }
    }

}
