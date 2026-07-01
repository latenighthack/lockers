package com.latenighthack.lockers.server

import assertk.assertThat
import assertk.assertions.isEqualTo
import com.latenighthack.lockers.server.tools.ShardedDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.test.Test

class ShardedDispatcherTest {
    @Test
    fun serializesWorkForTheSameId() = runBlocking {
        val dispatcher = ShardedDispatcher<String>(shardCount = 4, name = "test-shard") { it.hashCode() }
        var counter = 0

        // All work for "room-a" lands on one single-threaded shard, so these
        // non-atomic read-modify-writes must not lose updates.
        val jobs = (1..500).map {
            launch(Dispatchers.Default) {
                dispatcher.runOnDispatcher("room-a") {
                    counter += 1
                }
            }
        }
        jobs.joinAll()

        assertThat(counter).isEqualTo(500)
        dispatcher.close()
    }

    @Test
    fun returnsBlockResult() = runBlocking {
        val dispatcher = ShardedDispatcher<String>(shardCount = 2, name = "test-shard-2") { it.hashCode() }
        val result = dispatcher.runOnDispatcher("k") { 21 * 2 }
        assertThat(result).isEqualTo(42)
        dispatcher.close()
    }
}
