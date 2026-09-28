package com.latenighthack.lockers.server.tools

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

    suspend fun <U> runOnDispatcher(id: T, block: suspend () -> U): U {
        val shardIndex = (idHashCode(id) % shardCount + shardCount) % shardCount
        val dispatcher = shardDispatchers[shardIndex]

        // limitedParallelism(1) serializes execution segments, not whole suspending operations.
        // Hold a keyed mutex across suspension, while unrelated rooms remain independent.
        val entry = locks.compute(id) { _, existing -> (existing ?: Entry()).also { it.users++ } }!!
        try {
            return withContext(dispatcher) { entry.mutex.withLock { block() } }
        } finally {
            locks.computeIfPresent(id) { _, current -> if (--current.users == 0) null else current }
        }
    }

    fun dispatcherFor(id: T): CoroutineDispatcher = shardDispatchers[Math.floorMod(idHashCode(id), shardCount)]

    fun contextFor(id: T): CoroutineContext = dispatcherFor(id)

    /** Releases the backing thread pool. The dispatcher is unusable afterwards. */
    fun close() {
        dispatcher.close()
    }
}