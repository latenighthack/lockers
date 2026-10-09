package com.latenighthack.lockers.server

import com.latenighthack.ktstore.*
import com.latenighthack.ktbuf.net.RpcResponseException
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.SessionGatewayDiscovery
import com.latenighthack.lockers.server.agents.LockerAgentRegistry
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.session.v1.SessionGatewayService
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.io.File
import kotlinx.coroutines.runBlocking
import kotlin.test.*

class SnapshotPagingTest {
    @Test fun immutablePagesSurviveReopenAndRejectForeignExpiredOrUnboundedRequests() = runBlocking {
        val file = File.createTempFile("lockers-snapshot", ".db")
        val configuration = ServerStorage.configuration("snapshot-${file.name}")
        var db = createDatabase(configuration, file.absolutePath); db.open()
        val limits = ServerResourceLimits(maxLegacySnapshotLockers = 64, maxSnapshotLeases = 1)
        var now = 1000L
        val room = RoomId(byteArrayOf(1)); val session = SessionId(byteArrayOf(7))
        val records = (1..130).map { n -> IdentifiedLocker(LockerId(byteArrayOf((n ushr 8).toByte(), n.toByte())), Locker { open { encodedPayload = byteArrayOf(n.toByte()) } }, n.toLong()) }
        try {
            var snapshots = SnapshotStore(db, limits) { now }
            val first = snapshots.create(1, room, session, setOf(0), 64, 19, records)
            assertEquals(64, first.lockers.size); assertEquals(36, first.nextPageToken.size)
            // Closing/reopening represents a request routed to another replica;
            // the page stores hold immutable response bytes rather than a mutable scan.
            db.close(); db = createDatabase(configuration, file.absolutePath); db.open()
            snapshots = SnapshotStore(db, limits) { now }
            val second = snapshots.next(1, room, session, setOf(0), 64, first.nextPageToken)
            val third = snapshots.next(1, room, session, setOf(0), 64, second.nextPageToken)
            assertEquals(records, first.lockers + second.lockers + third.lockers)
            assertTrue(listOf(first, second, third).all { it.roomSequence == 19L }); assertTrue(third.nextPageToken.isEmpty())
            for (read in listOf<suspend () -> Unit>(
                { snapshots.next(0, room, session, setOf(0), 64, first.nextPageToken) },
                { snapshots.next(1, RoomId(byteArrayOf(2)), session, setOf(0), 64, first.nextPageToken) },
                { snapshots.next(1, room, SessionId(byteArrayOf(8)), setOf(0), 64, first.nextPageToken) },
                { snapshots.next(1, room, session, setOf(1), 64, first.nextPageToken) },
                { snapshots.next(1, room, session, setOf(0), 32, first.nextPageToken) })) {
                assertEquals(Codes.INVALID_ARGUMENT, assertFailsWith<RpcResponseException> { read() }.code)
            }
            assertEquals(Codes.RESOURCE_EXHAUSTED, assertFailsWith<RpcResponseException> { snapshots.create(1, room, session, setOf(0), 64, 20, records) }.code)
            assertEquals(Codes.RESOURCE_EXHAUSTED, assertFailsWith<RpcResponseException> { snapshots.create(0, room, null, emptySet(), 0, 0, records) }.code)
            now += limits.snapshotLeaseMillis
            assertEquals(Codes.INVALID_ARGUMENT, assertFailsWith<RpcResponseException> { snapshots.next(1, room, session, setOf(0), 64, first.nextPageToken) }.code)
            assertEquals(64, snapshots.create(1, room, session, setOf(0), 64, 20, records).lockers.size)
            snapshots.deleteAllForSession(session.rawValue)
            assertEquals(64, snapshots.create(1, room, session, setOf(0), 64, 21, records).lockers.size)
        } finally { db.close(); file.delete() }
    }
    @Test fun publicPagesKeepOriginalRowsAndWatermarkAcrossConcurrentChanges() = runBlocking {
        val db = ServerStorage.inMemory(); db.open(); val lockers = LockerStoreImpl(db)
        val service = RoomServiceImpl(SubscriptionStoreImpl(db), lockers, LockStoreImpl(db), object : SessionGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): SessionGatewayService? = null
        }, LocalRoomOwnership(), LockerAgentRegistry.None, SimpleMeterRegistry(), LockersConfig.defaults().copy(deliveryWorkerEnabled = false))
        val rpc = LocalRoomServiceRpc(service); val room = RoomId(byteArrayOf(5))
        fun row(n: Int, version: Long = n.toLong()) = ServerLocker(ServerRoomId(room.rawValue), 0,
            ServerLockerId(byteArrayOf((n ushr 8).toByte(), n.toByte())), Locker { open { encodedPayload = byteArrayOf(n.toByte()) } }.toByteArray(), version)
        try {
            lockers.updateLockers((1..130).map { row(it) })
            val first = rpc.getAllLockers(GetAllLockersRequest(roomId = room, pageSize = 64))
            assertEquals(64, first.lockers.size)
            lockers.updateLockers(listOf(row(1, 999), row(131)))
            val second = rpc.getAllLockers(GetAllLockersRequest(roomId = room, pageSize = 64, pageToken = first.nextPageToken))
            val third = rpc.getAllLockers(GetAllLockersRequest(roomId = room, pageSize = 64, pageToken = second.nextPageToken))
            assertEquals((1L..130L).toList(), (first.lockers + second.lockers + third.lockers).map { it.version })
            assertEquals(first.roomSequence, third.roomSequence)
            assertEquals(131, lockers.getAllLockers(ServerRoomId(room.rawValue)).size)
            assertTrue(rpc.capabilities(CapabilitiesRequest()).snapshotPaging)
        } finally { service.close(); db.close() }
    }

}
