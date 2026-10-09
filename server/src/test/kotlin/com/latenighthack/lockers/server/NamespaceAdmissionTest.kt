package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.net.RpcResponseException
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import kotlinx.coroutines.*
import kotlin.test.*

class NamespaceAdmissionTest {
    @Test fun configuredGraphSerializesGlobalLockerBudgetAndRetirementKeepsReservation() = runBlocking {
        val db = ServerStorage.inMemory(); db.open()
        val core = ServerCore::class.create(LockersConfig.defaults().copy(resourceLimits = ServerResourceLimits(maxLockers = 3, maxLockersPerRoom = 2)), db)
        fun row(n: Int, room: Int = n) = ServerLocker(ServerRoomId(byteArrayOf(room.toByte())), 0, ServerLockerId(byteArrayOf(n.toByte())), version = 1)
        try {
            val successes = coroutineScope { (1..10).map { n -> async {
                try { core.lockerStore.updateLocker(row(n)); n }
                catch (e: RpcResponseException) { assertEquals(Codes.RESOURCE_EXHAUSTED, e.code); null }
            } }.awaitAll().filterNotNull() }
            assertEquals(3, successes.size)
            val n = successes.first(); val id = ServerLockerId(byteArrayOf(n.toByte())); val room = ServerRoomId(byteArrayOf(n.toByte()))
            core.lockerStore.updateLocker(row(n).copy(version = 2))
            core.lockerStore.deleteLocker(room, 0, id)
            assertEquals(3, core.lockerStore.getLocker(room, 0, id)?.version)
            assertTrue(core.lockerStore.getLocker(room, 0, id)!!.deleted)
            assertEquals(Codes.RESOURCE_EXHAUSTED, assertFailsWith<RpcResponseException> { core.lockerStore.updateLocker(row(20)) }.code)
            assertFailsWith<RpcResponseException> { core.lockerStore.updateLockers(listOf(row(n).copy(version = 999), row(20))) }
            assertEquals(3, core.lockerStore.getLocker(room, 0, id)?.version)
        } finally { db.close() }
    }
    @Test fun retiredScopesAndPerRoomSubscriptionsRetainCorrectFiniteBudgets() = runBlocking {
        val db = ServerStorage.inMemory(); db.open()
        val limits = ServerResourceLimits(maxLocks = 2, maxLocksPerRoom = 1, maxSubscriptions = 2,
            maxSubscriptionsPerSession = 1, maxSubscriptionsPerRoom = 1)
        val locks = LockStoreImpl(db, limits); val subscriptions = SubscriptionStoreImpl(db, limits)
        val room = ServerRoomId(byteArrayOf(1)); val other = ServerRoomId(byteArrayOf(2)); val id = ServerLockerId(byteArrayOf(3))
        try {
            val lock = ServerLock(roomId = room, scopeKind = 2, keyspace = 0, lockerId = id,
                lockState = LockState(locked = true, lockVersion = 9).toByteArray())
            locks.saveLock(lock); locks.saveLock(lock)
            locks.deleteLock(room, 2, 0, id)
            assertEquals(9, LockState.fromByteArray(locks.getLock(room, 2, 0, id)!!.lockState).lockVersion)
            assertFalse(LockState.fromByteArray(locks.getLock(room, 2, 0, id)!!.lockState).locked)
            assertFailsWith<RpcResponseException> { locks.saveLock(lock.copy(lockerId = ServerLockerId(byteArrayOf(4)))) }
            locks.saveLock(lock.copy(roomId = other))
            assertFailsWith<RpcResponseException> { locks.saveLock(lock.copy(roomId = ServerRoomId(byteArrayOf(5)))) }
            val sid = ServerSessionId(byteArrayOf(6)); val second = ServerSessionId(byteArrayOf(7))
            subscriptions.addSubscription(sid, room); subscriptions.addSubscription(sid, room)
            assertFailsWith<RpcResponseException> { subscriptions.addSubscription(second, room) }
            assertFailsWith<RpcResponseException> { subscriptions.addSubscription(sid, other) }
            subscriptions.addSubscription(second, other)
            subscriptions.removeSubscription(sid, room)
            subscriptions.addSubscription(sid, ServerRoomId(byteArrayOf(8)))
            assertEquals(1, subscriptions.getAllSubscriptions(sid).size)
        } finally { db.close() }
    }
}
