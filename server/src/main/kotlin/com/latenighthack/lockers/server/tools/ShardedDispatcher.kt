package com.latenighthack.lockers.server.tools

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.withContext
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.ConcurrentHashMap

class ShardedDispatcher<T>(
    private val shardCount: Int,
    name: String,
    private val idHashCode: (T) -> Int
) {
    @OptIn(DelicateCoroutinesApi::class)
    private val dispatcher = newFixedThreadPoolContext(shardCount, name)
    private val shardDispatchers: Array<CoroutineDispatcher> = Array(shardCount) {
        dispatcher.limitedParallelism(1)
    }

    private class Entry(val mutex: Mutex = Mutex(), var users: Int = 0)
    private val locks = ConcurrentHashMap<T, Entry>()
    private val operations = HashSet<CompletableDeferred<Unit>>()
    private val gate = Any()
    private val closing = Mutex()
    private var closed = false

    suspend fun <U> runOnDispatcher(id: T, block: suspend () -> U): U = execute(id, true, block)

    /** For effects already serialized by a storage transaction: never wait on a key mutex while owning that transaction. */
    internal suspend fun <U> runWithoutKeyLock(id: T, block: suspend () -> U): U = execute(id, false, block)

    private suspend fun <U> execute(id: T, serialize: Boolean, block: suspend () -> U): U {
        val shardIndex = Math.floorMod(idHashCode(id), shardCount)
        val dispatcher = shardDispatchers[shardIndex]
        val ticket = CompletableDeferred<Unit>()
        synchronized(gate) { check(!closed) { "Sharded dispatcher is closed" }; operations.add(ticket) }
        var entry: Entry? = null
        try {
            entry = if (serialize) locks.compute(id) { _, existing -> (existing ?: Entry()).also { it.users++ } }!! else null
            val current = entry
            return withContext(dispatcher + ServiceLifecycle.context) {
                if (current == null) block() else current.mutex.withLock { block() }
            }
        } finally {
            if (entry != null) locks.computeIfPresent(id) { _, current -> if (--current.users == 0) null else current }
            synchronized(gate) { operations.remove(ticket); ticket.complete(Unit) }
        }
    }

    fun dispatcherFor(id: T): CoroutineDispatcher = synchronized(gate) {
        check(!closed) { "Sharded dispatcher is closed" }
        shardDispatchers[Math.floorMod(idHashCode(id), shardCount)]
    }

    fun contextFor(id: T): CoroutineContext = dispatcherFor(id)

    /** Releases the backing thread pool. The dispatcher is unusable afterwards. */
    suspend fun closeAndJoin() {
        ServiceLifecycle.requireExternalClose()
        closing.withLock {
            val pending = synchronized(gate) { closed = true; operations.toList() }
            kotlinx.coroutines.withContext(kotlinx.coroutines.NonCancellable) {
                pending.forEach { it.await() }
                dispatcher.close()
            }
        }
    }
    fun close() = ServiceLifecycle.blockingClose { closeAndJoin() }
}
