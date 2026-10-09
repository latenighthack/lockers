package com.latenighthack.lockers.connector.test

import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.room.v1.*
import kotlinx.coroutines.*
import kotlin.test.*

class ReviewIdentityAliasTests {
    private val unknown = byteArrayOf(0xa0.toByte(), 0x06, 0x01)
    @Test fun `unknown identity fields cannot create a second mutation lane`(): Unit = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val sync = LockerSyncCoordinator(scope)
        val room = RoomId(byteArrayOf(1)); val id = LockerId(byteArrayOf(2))
        val aliasRoom = RoomId.fromByteArray(room.toByteArray() + unknown)
        val aliasId = LockerId.fromByteArray(id.toByteArray() + unknown)
        val entered = CompletableDeferred<Unit>(); val release = CompletableDeferred<Unit>(); val aliasEntered = CompletableDeferred<Unit>()
        try {
            val first = async { sync.mutate(room to id) { entered.complete(Unit); release.await() } }
            entered.await()
            val second = async { sync.mutate(aliasRoom to aliasId) { aliasEntered.complete(Unit) } }
            assertNull(withTimeoutOrNull(100) { aliasEntered.await() }, "logical alias bypassed the held mutation lane")
            release.complete(Unit); first.await(); second.await()
        } finally { release.complete(Unit); scope.coroutineContext[Job]!!.cancelAndJoin() }
    }
    @Test fun `duplicate aliased locker batch is rejected before RPC`(): Unit = runBlocking {
        val id = LockerId(byteArrayOf(2), LockerKeyspace(0))
        val alias = LockerId.fromByteArray(id.toByteArray() + unknown)
        var calls = 0
        val client = reviewClient(ReviewRpc { method, _ -> when (method.methodName) {
            "Capabilities" -> CapabilitiesResponse(postLockerChanges = true).toByteArray()
            "PostLockerChanges" -> { calls++; PostLockerChangesResponse().toByteArray() }
            else -> error(method.methodName)
        } })
        try {
            assertFailsWith<IllegalArgumentException> { client.updateLockers(RoomId(byteArrayOf(1)), listOf(
                LockerClient.Change(id) { byteArrayOf(1) }, LockerClient.Change(alias) { byteArrayOf(2) })) }
            assertEquals(0, calls)
        } finally { client.closeAndJoin() }
    }
    @Test fun `canonical identity snapshots mutable caller bytes and unknown keyspace fields`() {
        val raw = byteArrayOf(2)
        val decoratedSpace = LockerKeyspace.fromByteArray(LockerKeyspace(7).toByteArray() + unknown)
        val canonical = LockerId(raw, decoratedSpace).canonical()
        raw.fill(9)
        assertContentEquals(byteArrayOf(2), canonical.rawValue)
        assertEquals(LockerKeyspace(7).hashCode(), canonical.keyspace.hashCode())
    }
}
