package com.latenighthack.lockers.connector

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/** One instance per LockerClient; work belongs to the client, not the first waiter. */
internal class LockerSyncCoordinator(private val scope: CoroutineScope) {
    private val mutex = Mutex()
    private val reads = mutableMapOf<Any, Deferred<Any?>>()
    private val mutations = mutableMapOf<Any, Mutex>()
    private val gate = PriorityNetworkGate(4)

    @Suppress("UNCHECKED_CAST")
    suspend fun <T> read(key: Any, operation: suspend () -> T): T {
        val work = mutex.withLock {
            reads[key] ?: scope.async(start = CoroutineStart.LAZY) {
                try { network(false, operation) }
                finally { mutex.withLock { reads.remove(key) } }
            }.also { reads[key] = it; it.start() }
        }
        return work.await() as T
    }
    suspend fun <T> mutate(key: Any, operation: suspend () -> T): T {
        val lock = mutex.withLock { mutations.getOrPut(key) { Mutex() } }
        return lock.withLock { operation() }
    }
    suspend fun <T> network(interactive: Boolean = true, operation: suspend () -> T): T = gate.run(interactive, operation)
}

private class PriorityNetworkGate(private val limit: Int) {
    private val mutex = Mutex()
    private var active = 0
    private val writes = ArrayDeque<CompletableDeferred<Unit>>()
    private val reads = ArrayDeque<CompletableDeferred<Unit>>()
    suspend fun <T> run(interactive: Boolean, operation: suspend () -> T): T {
        val permit = CompletableDeferred<Unit>()
        mutex.withLock {
            if (active < limit) { active++; permit.complete(Unit) }
            else (if (interactive) writes else reads).addLast(permit)
        }
        try { permit.await() }
        catch (e: CancellationException) {
            withContext(NonCancellable) { mutex.withLock {
                if (!writes.remove(permit) && !reads.remove(permit)) release()
            } }
            throw e
        }
        try { return operation() }
        finally { withContext(NonCancellable) { mutex.withLock { release() } } }
    }
    private fun release() {
        val next = if (writes.isNotEmpty()) writes.removeFirst() else if (reads.isNotEmpty()) reads.removeFirst() else null
        if (next == null) active-- else next.complete(Unit)
    }
}
