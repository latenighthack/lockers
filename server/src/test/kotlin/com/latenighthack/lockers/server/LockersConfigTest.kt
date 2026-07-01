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
}
