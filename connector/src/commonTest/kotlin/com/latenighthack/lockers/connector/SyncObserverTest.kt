package com.latenighthack.lockers.connector

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class SyncObserverTest {
    @Test fun observerFailureDoesNotChangeResultOrCancellation() = runTest {
        val observer = SyncObserver { _, _, _, _, _ -> error("exporter unavailable") }
        assertEquals(42, observer.measure("persist") { 42 })
        assertFailsWith<CancellationException> {
            observer.measure("accepted") {
                throw CancellationException("caller stopped")
            }
        }
        val coordinator = LockerSyncCoordinator(backgroundScope, observer = observer)
        assertEquals(7, coordinator.network { 7 })
        assertEquals(8, coordinator.mutate("key") { 8 })
    }

    @Test fun cancelledQueueWaiterReleasesAdmissionAndReportsCancellation() = runTest {
        val observations = mutableListOf<Pair<String, String>>()
        val coordinator =
            LockerSyncCoordinator(
                backgroundScope,
                observer =
                SyncObserver { stage, outcome, _, _, _ ->
                    observations.add(
                        stage to outcome,
                    )
                },
            )
        val release = CompletableDeferred<Unit>()
        val occupied = List(4) {
            async(start = CoroutineStart.UNDISPATCHED) {
                coordinator.network {
                    release.await()
                }
            }
        }
        val cancelled = launch(start = CoroutineStart.UNDISPATCHED) {
            coordinator.network(false) {
                error("cancelled waiter ran")
            }
        }
        cancelled.cancelAndJoin()
        release.complete(Unit)
        occupied.awaitAll()
        assertEquals(42, coordinator.network { 42 })
        assertTrue("background_wait" to "cancelled" in observations)
    }
}
