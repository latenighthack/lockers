package com.latenighthack.lockers.sharding

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isGreaterThan
import assertk.assertions.isGreaterThanOrEqualTo
import assertk.assertions.isLessThan
import kotlin.test.Test

class PartitioningTest {
    private val pf = HashPartitionFunction()

    @Test
    fun `partition stays within range`() {
        val n = 256
        repeat(1000) { i ->
            val shard = pf.partition(Keyspace(1), "room-$i".encodeToByteArray(), n).value
            assertThat(shard).isGreaterThanOrEqualTo(0)
            assertThat(shard).isLessThan(n)
        }
    }

    @Test
    fun `partition is deterministic across instances`() {
        val a = HashPartitionFunction()
        val b = HashPartitionFunction()
        repeat(500) { i ->
            val key = "k-$i".encodeToByteArray()
            assertThat(b.partition(Keyspace(7), key, 128))
                .isEqualTo(a.partition(Keyspace(7), key, 128))
        }
    }

    @Test
    fun `keyspace independence — same key maps differently per keyspace`() {
        var differing = 0
        repeat(1000) { i ->
            val key = "room-$i".encodeToByteArray()
            if (pf.partition(Keyspace(1), key, 256) != pf.partition(Keyspace(2), key, 256)) {
                differing++
            }
        }
        // Folding the keyspace into the hash makes the two mappings essentially independent.
        assertThat(differing).isGreaterThan(900)
    }
}
