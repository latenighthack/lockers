package com.latenighthack.lockers.sharding

import assertk.assertThat
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import kotlin.test.Test
import kotlin.test.assertFailsWith

class ShardCountsTest {
    @Test
    fun `parse applies overrides with default fallback`() {
        val c = ShardCounts.parse(256, "1=512,30=128")
        assertThat(c.forKeyspace(Keyspace(1))).isEqualTo(512)
        assertThat(c.forKeyspace(Keyspace(30))).isEqualTo(128)
        assertThat(c.forKeyspace(Keyspace(99))).isEqualTo(256)
    }

    @Test
    fun `blank or null spec yields default only`() {
        assertThat(ShardCounts.parse(64, null).forKeyspace(Keyspace(1))).isEqualTo(64)
        assertThat(ShardCounts.parse(64, "   ").perKeyspace).isEmpty()
    }

    @Test
    fun `malformed spec fails fast`() {
        assertFailsWith<IllegalArgumentException> { ShardCounts.parse(256, "1=") }
        assertFailsWith<IllegalArgumentException> { ShardCounts.parse(256, "x=10") }
        assertFailsWith<IllegalArgumentException> { ShardCounts.parse(256, "1=0") }
        assertFailsWith<IllegalArgumentException> { ShardCounts.parse(256, "1:10") }
    }

    @Test
    fun `non-positive default is rejected`() {
        assertFailsWith<IllegalArgumentException> { ShardCounts(0) }
    }
}
