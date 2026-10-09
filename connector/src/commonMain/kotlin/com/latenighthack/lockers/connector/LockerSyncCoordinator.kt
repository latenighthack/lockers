package com.latenighthack.lockers.connector

import com.latenighthack.lockers.observability.*
import com.latenighthack.lockers.common.v1.*

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/** One instance per LockerClient; work belongs to the client, not the first waiter. */
internal class LockerSyncCoordinator(private val scope: CoroutineScope,
    private val telemetry: LockersTelemetry = LockersTelemetry.NONE, private val observer: SyncObserver =
        SyncObserver.NONE) {
    private val mutex = Mutex()
    private val reads = mutableMapOf<Any, Deferred<Any?>>()
    private class Mutation(val lock: Mutex = Mutex(), var users: Int = 0)
    private var mutationUsers = 0
    private val mutations = mutableMapOf<Any, Mutation>()
    private val gate = PriorityNetworkGate(4, telemetry, observer)

    @Suppress("UNCHECKED_CAST")
    suspend fun <T> read(key: Any, operation: suspend () -> T): T {
        val key = requireNotNull(canonicalKey(key))
        val tracing = currentCoroutineContext()[TelemetryContext]?.tracing ?: kotlin.coroutines.EmptyCoroutineContext
        var coalescedDepth: Int? = null
        val work = mutex.withLock {
            reads[key]?.also { coalescedDepth = reads.size } ?: run { check(reads.size < 1_024) {
                "Read admission limit exceeded" }; scope.async(tracing, start = CoroutineStart.LAZY) {
                try { network(false, operation) }
                finally { withContext(NonCancellable) { mutex.withLock { reads.remove(key) } } }
            }.also { reads[key] = it; it.start() } }
        }
        coalescedDepth?.let { observer.record("coalesced_read", depth = it) }
        return work.await() as T
    }
    suspend fun <T> mutate(key: Any, operation: suspend () -> T): T {
        val key = requireNotNull(canonicalKey(key))
        val entry = mutex.withLock {
            check(mutationUsers < 1_024) { "Mutation admission limit exceeded" }
            mutations.getOrPut(key) { Mutation() }.also { it.users++; mutationUsers++ }
        }
        try {
            observer.measure("mutation_wait") { entry.lock.lock() }
            try { return operation() } finally { entry.lock.unlock() }
        }
        finally { withContext(NonCancellable) { mutex.withLock { mutationUsers--; if (--entry.users == 0 && mutations[key] === entry) mutations.remove(key) } } }
    }
    suspend fun <T> network(interactive: Boolean = true, operation: suspend () -> T): T = gate.run(interactive, operation)
}

private class PriorityNetworkGate(private val limit: Int, private val telemetry: LockersTelemetry,
    private val observer: SyncObserver) {
    private val mutex = Mutex()
    private var active = 0
    private val writes = ArrayDeque<CompletableDeferred<Unit>>()
    private val reads = ArrayDeque<CompletableDeferred<Unit>>()
    suspend fun <T> run(interactive: Boolean, operation: suspend () -> T): T {
        val stage = if (interactive) "interactive_wait" else "background_wait"
        val permit = CompletableDeferred<Unit>()
        val depth = mutex.withLock {
            if (active < limit) { active++; permit.complete(Unit) }
            else {
                check(writes.size + reads.size < 256) { "Network queue admission limit exceeded" }
                (if (interactive) writes else reads).addLast(permit)
            }
            writes.size + reads.size
        }
        observer.record(stage, depth = depth, capacity = limit)
        try { observer.measure(stage) { telemetry.observe(TelemetryOperation.CONNECTOR_QUEUE) { permit.await() } } }
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

private fun canonicalKey(key: Any?): Any? = when (key) {
    is RoomId -> key.canonical()
    is LockerId -> key.canonical()
    is LockerKeyspace -> key.canonical()
    is SessionId -> key.canonical()
    is Pair<*, *> -> canonicalKey(key.first) to canonicalKey(key.second)
    is Triple<*, *, *> -> Triple(canonicalKey(key.first), canonicalKey(key.second), canonicalKey(key.third))
    is List<*> -> key.map(::canonicalKey)
    else -> key
}
