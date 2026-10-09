package com.latenighthack.lockers.connector.test

import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.*
import io.ktor.server.application.Application
import kotlinx.coroutines.*
import kotlin.test.*

class ReviewBatchCreationTests {
    @Test fun bulkAbsentLookupMatchesSingleVersionZeroAuthority() = runOwnedTestWithServer(Application::attachTestServices) { server, _ ->
        val rpc = RoomServiceRpc(server.ownedRpcClient)
        val room = RoomId(byteArrayOf(1)); val id = LockerId(byteArrayOf(2), LockerKeyspace(3))
        val single = rpc.getLocker(GetLockerRequest(room, id))
        val bulk = rpc.getLockers(GetLockersRequest(room, listOf(id))).results.single()
        assertTrue(bulk.result.isOk(), "Absent bulk identities must carry their authority at version zero")
        assertEquals(single, bulk)
    }
    @Test fun ordinaryAtomicBatchCreatesFreshUnclaimedLockers() = runOwnedTestWithServer(Application::attachTestServices) { server, _ -> withContext(Dispatchers.Default) {
        val key = Secp256r1KeyPair.generate()
        val client = createOwnedTestClient(server.ownedRpcClient, ConnectorStorage.inMemory(), KeyValueStore(InMemoryKeyValueStoreDelegate()), object : AuthenticationKeySource {
            override suspend fun getSessionKeyPair() = key
            override suspend fun hasSessionKeyPair() = true
            override suspend fun generateSessionKeyPair() {}
            override suspend fun revokeKeys() {}
        }, Version())
        try {
        withTimeout(5000) { client.awaitConnected() }
        val room = RoomId(byteArrayOf(4)); val ids = (1..2).map { LockerId(byteArrayOf(it.toByte()), LockerKeyspace(5)) }
        withTimeout(5000) { client.lockers.updateLockers(room, ids.map { id -> LockerClient.Change(id) { byteArrayOf(9) } }) }
        val actual = RoomServiceRpc(server.ownedRpcClient).getLockers(GetLockersRequest(room, ids))
        assertEquals(2, actual.results.size)
        for (result in actual.results) {
            assertTrue(result.result.isOk()); assertEquals(1, result.locker!!.version)
            assertContentEquals(byteArrayOf(9), result.locker!!.locker!!.open!!.encodedPayload)
        }
        } finally { client.closeAndJoin() }
    } }
}
