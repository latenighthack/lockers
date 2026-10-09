package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.net.RpcResponseException
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.lockers.server.services.room.v1.LockerStoreImpl
import com.latenighthack.lockers.server.services.room.v1.LockStoreImpl
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import java.sql.DriverManager
import kotlinx.coroutines.*
import org.junit.jupiter.api.Assumptions.assumeTrue
import kotlin.test.*

class NamespaceAdmissionPgTest {
    @Test fun twoPostgresHandlesShareOneGlobalReservationBudget() = runBlocking {
        val base = System.getenv("LOCKERS_POSTGRES_TEST_URL"); assumeTrue(base != null, "Set LOCKERS_POSTGRES_TEST_URL for real PostgreSQL coverage")
        val schema = "namespace_${System.nanoTime()}"
        DriverManager.getConnection(base).use { connection -> connection.createStatement().use { it.execute("CREATE SCHEMA $schema") } }
        val location = base + (if (base!!.contains('?')) "&" else "?") + "currentSchema=$schema"
        val a = ServerStorage.postgres(location); val b = ServerStorage.postgres(location)
        val limits = ServerResourceLimits(maxLockers = 3, maxLockersPerRoom = 1, maxLocks = 3, maxLocksPerRoom = 1)
        try {
            a.open(); b.open(); val stores = listOf(LockerStoreImpl(a, limits), LockerStoreImpl(b, limits))
            val accepted = coroutineScope { (1..12).map { n -> async(Dispatchers.Default) {
                try {
                    stores[n % 2].updateLocker(ServerLocker(ServerRoomId(byteArrayOf(n.toByte())), 0, ServerLockerId(byteArrayOf(1)), version = 1)); true
                } catch (e: RpcResponseException) { assertEquals(Codes.FAILED_PRECONDITION, e.code); false }
            } }.awaitAll() }
            assertEquals(3, accepted.count { it })
            val locks = listOf(LockStoreImpl(a, limits), LockStoreImpl(b, limits))
            val admittedLocks = coroutineScope { (1..12).map { n -> async(Dispatchers.Default) {
                try {
                    locks[n % 2].saveLock(ServerLock(roomId = ServerRoomId(byteArrayOf(n.toByte())), scopeKind = 1,
                        lockerId = ServerLockerId(byteArrayOf()), lockState = LockState(locked = true, lockVersion = 1).toByteArray())); true
                } catch (e: RpcResponseException) { assertEquals(Codes.FAILED_PRECONDITION, e.code); false }
            } }.awaitAll() }
            assertEquals(3, admittedLocks.count { it })
        } finally {
            a.close(); b.close()
            DriverManager.getConnection(base).use { connection -> connection.createStatement().use { it.execute("DROP SCHEMA $schema CASCADE") } }
        }
    }
}
