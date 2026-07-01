package com.latenighthack.lockers.server

/**
 * APNS push configuration. Credentials are optional; when absent the push
 * service logs a warning and no-ops rather than failing.
 */
data class ApnsConfig(
    val teamId: String?,
    val keyId: String?,
    val keyPath: String?,
    val environment: String,
    val topic: String,
) {
    val isConfigured: Boolean get() = teamId != null && keyId != null && keyPath != null
}

/**
 * All runtime tunables, read once from the environment at startup.
 *
 * Environment variables (12-factor style) are the source of truth so the same
 * artifact runs unchanged across environments; every value has a safe default
 * for local development.
 *
 *   LOCKERS_HTTP_PORT            HTTP port (default 8080)
 *   LOCKERS_DB_URL               Postgres JDBC URL; unset => in-memory (dev only, not durable)
 *   LOCKERS_MAX_LOCKER_BYTES     max serialized locker payload (default 1 MiB)
 *   LOCKERS_ROOM_WRITES_PER_SEC  per-room sustained write rate (default 50; <=0 disables)
 *   LOCKERS_ROOM_WRITE_BURST     per-room burst allowance (default 100)
 *   LOCKERS_SESSION_CACHE_SIZE   room->sessions cache entries (default 1000)
 *   LOCKERS_SHARD_MULTIPLIER     shard count = cpuCores * this (default 4)
 *   APNS_TEAM_ID / APNS_KEY_ID / APNS_KEY_PATH / APNS_ENVIRONMENT
 *   APNS_TOPIC                   push topic (default com.latenighthack.lockers)
 */
data class LockersConfig(
    val httpPort: Int,
    val databaseUrl: String?,
    val maxLockerPayloadBytes: Int,
    val roomWritesPerSecond: Int,
    val roomWriteBurst: Int,
    val sessionCacheSize: Long,
    val shardMultiplier: Int,
    val apns: ApnsConfig,
) {
    val shardCount: Int get() = (Runtime.getRuntime().availableProcessors() * shardMultiplier).coerceAtLeast(1)

    companion object {
        fun fromEnv(env: (String) -> String? = System::getenv): LockersConfig {
            fun int(name: String, default: Int) = env(name)?.trim()?.toIntOrNull() ?: default
            fun long(name: String, default: Long) = env(name)?.trim()?.toLongOrNull() ?: default
            return LockersConfig(
                httpPort = int("LOCKERS_HTTP_PORT", 8080),
                databaseUrl = env("LOCKERS_DB_URL")?.takeIf { it.isNotBlank() },
                maxLockerPayloadBytes = int("LOCKERS_MAX_LOCKER_BYTES", 1 * 1024 * 1024),
                roomWritesPerSecond = int("LOCKERS_ROOM_WRITES_PER_SEC", 50),
                roomWriteBurst = int("LOCKERS_ROOM_WRITE_BURST", 100),
                sessionCacheSize = long("LOCKERS_SESSION_CACHE_SIZE", 1000),
                shardMultiplier = int("LOCKERS_SHARD_MULTIPLIER", 4),
                apns = ApnsConfig(
                    teamId = env("APNS_TEAM_ID"),
                    keyId = env("APNS_KEY_ID"),
                    keyPath = env("APNS_KEY_PATH"),
                    environment = env("APNS_ENVIRONMENT") ?: "development",
                    topic = env("APNS_TOPIC") ?: "com.latenighthack.lockers",
                ),
            )
        }

        /** All-defaults config for tests and local embedding (no environment reads). */
        fun defaults(): LockersConfig = fromEnv { null }
    }
}
