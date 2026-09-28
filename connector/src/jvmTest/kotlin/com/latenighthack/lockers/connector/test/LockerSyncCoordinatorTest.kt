package com.latenighthack.lockers.connector.test

import com.latenighthack.lockers.connector.LockerSyncCoordinator
import kotlinx.coroutines.*
import kotlin.test.*

class LockerSyncCoordinatorTest {
    @Test fun interactiveRequestGetsNextSlotAheadOfQueuedHydration(): Unit = runBlocking {
        val coordinator = LockerSyncCoordinator(this)
        val release = kotlinx.coroutines.channels.Channel<Unit>()
        val occupied = (0 until 4).map { async(start = CoroutineStart.UNDISPATCHED) { coordinator.network(false) { release.receive() } } }
        val order = mutableListOf<String>()
        val background = async(start = CoroutineStart.UNDISPATCHED) { coordinator.network(false) { order.add("read") } }
        val write = async(start = CoroutineStart.UNDISPATCHED) { coordinator.network(true) { order.add("write") } }
        release.send(Unit)
        write.await(); background.await()
        assertEquals(listOf("write", "read"), order)
        repeat(3) { release.send(Unit) }
        occupied.awaitAll()
    }

    @Test fun cancellationOfFirstWaiterDoesNotCancelSharedRead() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val coordinator = LockerSyncCoordinator(scope)
        val entered = CompletableDeferred<Unit>()
        val finish = CompletableDeferred<Unit>()
        var calls = 0
        try {
            val first = async { coordinator.read("room") { calls++; entered.complete(Unit); finish.await(); 42 } }
            entered.await()
            val second = async(start = CoroutineStart.UNDISPATCHED) { coordinator.read<Int>("room") { error("duplicate hydration") } }
            first.cancelAndJoin()
            finish.complete(Unit)
            assertEquals(42, second.await())
            assertEquals(1, calls)
        } finally { scope.cancel() }
    }
    @Test fun sameLockerMutationsRemainSerializedAcrossSuspension() = runBlocking {
        val coordinator = LockerSyncCoordinator(this)
        var version = 0
        (0 until 20).map { async { coordinator.mutate("locker") { val read = version; yield(); version = read + 1 } } }.awaitAll()
        assertEquals(20, version)
    }
}
