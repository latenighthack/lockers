package com.latenighthack.lockers.server

import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.ktcrypto.Secp256r1KeyPair
import com.latenighthack.ktcrypto.generate
import kotlinx.coroutines.runBlocking
import kotlin.test.*

class UnknownIdentityAliasTest {
    @Test fun bulkReadsExposeAuthorityForUnwrittenLockers(): Unit = runBlocking {
        fixture { rpc ->
            val key = Secp256r1KeyPair.generate()
            assertTrue(rpc.lockLocker(LockLockerRequest(roomId = room, grant = LockGrant(
                scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM),
                publicKey = Secp256R1Key.PublicKey(key.publicKey.encode()),
            ))).result.isOk())
            val ids = listOf(id, LockerId(byteArrayOf(3), LockerKeyspace(7)), id)
            val bulk = rpc.getLockers(GetLockersRequest(room, ids)).results
            assertEquals(ids.size, bulk.size)
            ids.zip(bulk).forEach { (requested, result) ->
                assertEquals(rpc.getLocker(GetLockerRequest(room, requested)), result)
                assertTrue(result.result.isOk())
                val absent = assertNotNull(result.locker)
                assertEquals(0, absent.version)
                assertNull(absent.locker)
                assertEquals(1, assertNotNull(absent.lockState).lockVersion)
            }
        }
    }
    @Test fun duplicateRawLockerIdentityCannotBypassBatchAdmission(): Unit = runBlocking {
        fixture { rpc ->
            val base = change()
            val alias = base.copy(lockerId = base.lockerId!!.copy(unknownFields = byteArrayOf(0x98.toByte(), 6, 1)))
            val response = rpc.postLockerChanges(PostLockerChangesRequest(roomId = room, changes = listOf(base, alias), writeRequestId = ByteArray(16) { 7 }))
            assertEquals(PostLockerChangesResponse.Result.INVALID, response.result)
            val missing = rpc.getLocker(GetLockerRequest(room, id)).locker!!
            assertEquals(0, missing.version)
            assertNull(missing.locker)
        }
    }
    @Test fun rawLockerIdentityIsCanonicalForPrefetchAndBulkReads(): Unit = runBlocking {
        fixture { rpc ->
            assertEquals(1, rpc.postLockerChange(change()).version)
            val alias = id.copy(unknownFields = byteArrayOf(0x98.toByte(), 6, 1), keyspace = LockerKeyspace(0, unknownFields = byteArrayOf(0x98.toByte(), 6, 2)))
            val response = rpc.getLockers(GetLockersRequest(room, listOf(alias)))
            assertEquals(1, response.results.single().locker!!.version)
            assertEquals(2, rpc.postLockerChange(change().copy(lockerId = alias, parentVersion = 1)).version)
        }
    }
    private val room = RoomId(byteArrayOf(1))
    private val id = LockerId(byteArrayOf(2))
    private fun change() = PostLockerChangeRequest(roomId = room, lockerId = id, locker = Locker { open { encodedPayload = byteArrayOf(3) } })
    private suspend fun fixture(block: suspend (RoomService) -> Unit) {
        val db = ServerStorage.inMemory()
        val core = ServerCore::class.create(LockersConfig.defaults().copy(deliveryWorkerEnabled = false), db)
        core.overridePushProviders = emptyList()
        core.setup()
        val component = MonolithComponent(core)
        try { block(LocalRoomServiceRpc(component.roomServiceModule.serverImpl)) }
        finally { component.closeAndJoin(); db.close() }
    }
}
