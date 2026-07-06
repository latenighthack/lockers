package com.latenighthack.lockers.server

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isNull
import assertk.assertions.isTrue
import kotlin.test.Test

class LockersConfigTest {
    @Test
    fun appliesSafeDefaultsWhenUnset() {
        val c = LockersConfig.fromEnv { null }
        assertThat(c.httpPort).isEqualTo(8080)
        assertThat(c.databaseUrl).isNull()
        assertThat(c.maxLockerPayloadBytes).isEqualTo(1024 * 1024)
        assertThat(c.roomWritesPerSecond).isEqualTo(50)
        assertThat(c.apns.isConfigured).isFalse()
        assertThat(c.apns.topic).isEqualTo("com.latenighthack.lockers")
    }

    @Test
    fun readsEnvironmentOverrides() {
        val env = mapOf(
            "LOCKERS_HTTP_PORT" to "9090",
            "LOCKERS_DB_URL" to "jdbc:postgresql://h:5432/db",
            "LOCKERS_MAX_LOCKER_BYTES" to "2048",
            "LOCKERS_ROOM_WRITES_PER_SEC" to "5",
            "APNS_TEAM_ID" to "T",
            "APNS_KEY_ID" to "K",
            "APNS_KEY_PATH" to "/tmp/k.p8",
            "APNS_TOPIC" to "com.example.app",
        )
        val c = LockersConfig.fromEnv { env[it] }
        assertThat(c.httpPort).isEqualTo(9090)
        assertThat(c.databaseUrl).isEqualTo("jdbc:postgresql://h:5432/db")
        assertThat(c.maxLockerPayloadBytes).isEqualTo(2048)
        assertThat(c.roomWritesPerSecond).isEqualTo(5)
        assertThat(c.apns.isConfigured).isTrue()
        assertThat(c.apns.topic).isEqualTo("com.example.app")
    }

    @Test
    fun blankDatabaseUrlIsTreatedAsUnset() {
        val c = LockersConfig.fromEnv { name -> if (name == "LOCKERS_DB_URL") "   " else null }
        assertThat(c.databaseUrl).isNull()
    }

    @Test
    fun defaultsEqualsFromEnvNull() {
        // The monolith contract: defaults() must stay a valid, sharding-disabled config identical
        // to reading an empty environment. Guards against a new required env var creeping in.
        assertThat(LockersConfig.defaults()).isEqualTo(LockersConfig.fromEnv { null })
    }

    @Test
    fun shardingDefaultsDisableClusterMode() {
        val c = LockersConfig.fromEnv { null }
        assertThat(c.clusterEnabled).isFalse()
        assertThat(c.sharding.nodeId).isNull()
        assertThat(c.sharding.peers).isNull()
        assertThat(c.sharding.advertiseAddr).isNull()
        assertThat(c.sharding.shardCountDefault).isEqualTo(256)
        assertThat(c.sharding.ringVnodes).isEqualTo(128)
        assertThat(c.sharding.sessionShardCount).isEqualTo(256)
        assertThat(c.requireDb).isFalse()
    }

    @Test
    fun clusterEnabledRequiresBothPeersAndNodeId() {
        // Peers without a node id is NOT a cluster (and vice versa) — a single stray var must not
        // flip the monolith into a mis-configured cluster.
        assertThat(LockersConfig.fromEnv { mapOf("LOCKERS_PEERS" to "a:8080")[it] }.clusterEnabled).isFalse()
        assertThat(LockersConfig.fromEnv { mapOf("LOCKERS_NODE_ID" to "a")[it] }.clusterEnabled).isFalse()

        val env = mapOf("LOCKERS_PEERS" to "a=a:8080,b=b:8080", "LOCKERS_NODE_ID" to "a")
        assertThat(LockersConfig.fromEnv { env[it] }.clusterEnabled).isTrue()
    }

    @Test
    fun readsShardingOverrides() {
        val env = mapOf(
            "LOCKERS_NODE_ID" to "lockers-1",
            "LOCKERS_ADVERTISE_ADDR" to "lockers-1.internal:8080",
            "LOCKERS_PEERS" to "lockers-0=lockers-0.internal:8080,lockers-1=lockers-1.internal:8080",
            "LOCKERS_SHARD_COUNT_DEFAULT" to "512",
            "LOCKERS_KEYSPACE_SHARD_COUNTS" to "1=64,30=128",
            "LOCKERS_RING_VNODES" to "64",
            "LOCKERS_SESSION_SHARD_COUNT" to "128",
            "LOCKERS_REQUIRE_DB" to "true",
        )
        val c = LockersConfig.fromEnv { env[it] }
        assertThat(c.sharding.nodeId).isEqualTo("lockers-1")
        assertThat(c.sharding.advertiseAddr).isEqualTo("lockers-1.internal:8080")
        assertThat(c.sharding.shardCountDefault).isEqualTo(512)
        assertThat(c.sharding.keyspaceShardCounts).isEqualTo("1=64,30=128")
        assertThat(c.sharding.ringVnodes).isEqualTo(64)
        assertThat(c.sharding.sessionShardCount).isEqualTo(128)
        assertThat(c.requireDb).isTrue()
        assertThat(c.clusterEnabled).isTrue()
    }
}
