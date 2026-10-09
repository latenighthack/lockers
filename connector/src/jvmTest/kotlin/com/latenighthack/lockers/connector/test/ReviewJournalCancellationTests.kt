package com.latenighthack.lockers.connector.test

import com.latenighthack.lockers.connector.ConnectorStorage
import com.latenighthack.lockers.connector.internal.LockerStoreImpl
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.first
import kotlin.test.*

class ReviewJournalCancellationTests {
    @Test fun `already cancelled journal collection propagates cancellation instead of completing empty`() = runBlocking {
        val database = ConnectorStorage.inMemory(); database.open()
        val store = LockerStoreImpl(database)
        val observed = CompletableDeferred<Throwable>()
        try {
            val collector = launch(start = CoroutineStart.UNDISPATCHED) {
                currentCoroutineContext().cancel()
                try {
                    store.changesAfter(0).first()
                    observed.complete(AssertionError("Cancelled collection returned a value"))
                } catch (failure: Throwable) { observed.complete(failure) }
            }
            collector.join()
            assertIs<CancellationException>(observed.await())
            Unit
        } finally { database.close() }
    }
}
