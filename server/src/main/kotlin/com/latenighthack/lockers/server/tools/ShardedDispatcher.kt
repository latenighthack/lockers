package com.latenighthack.lockers.server.tools

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.withContext
import kotlin.coroutines.CoroutineContext

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

    suspend fun <U> runOnDispatcher(id: T, block: suspend () -> U): U {
        val shardIndex = (idHashCode(id) % shardCount + shardCount) % shardCount
        val dispatcher = shardDispatchers[shardIndex]

        return withContext(dispatcher) {
            block()
        }
    }

    fun dispatcherFor(id: T): CoroutineDispatcher = shardDispatchers[idHashCode(id)]

    fun contextFor(id: T): CoroutineContext = dispatcherFor(id)
}