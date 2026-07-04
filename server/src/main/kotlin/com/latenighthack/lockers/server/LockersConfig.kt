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
 * FCM (Android) push configuration. [credentialsPath] points at a service-account
 * JSON file; absent => the FCM backend no-ops.
 */
data class FcmConfig(
    val credentialsPath: String?,
) {
    val isConfigured: Boolean get() = !credentialsPath.isNullOrBlank()
}

/**
 * Web Push (VAPID) configuration. The VAPID key pair is carried as base64url
 * strings of the uncompressed P-256 point / raw private scalar; [subject] is the
 * VAPID `sub` claim (a `mailto:` or origin URL). Absent => the web-push backend
 * no-ops.
 */
data class WebPushConfig(
    val vapidPublicKey: String?,
    val vapidPrivateKey: String?,
    val subject: String?,
) {
    val isConfigured: Boolean
        get() = !vapidPublicKey.isNullOrBlank() && !vapidPrivateKey.isNullOrBlank() && !subject.isNullOrBlank()
}

/**
 * All runtime tunables, read once from the environment at startup.
 *
 * Environment variables (12-factor style) are the source of truth so the same
 * artifact runs unchanged across environments; every value has a safe default
 * for local development.
 *
 *   LOCKERS_HTTP_PORT            public client HTTP port (default 8080)
 *   LOCKERS_ADMIN_PORT           internal admin HTTP port; bind cluster-internal (default 8081)
 *   LOCKERS_ADMIN_TOKEN          if set, required as the `x-admin-token` header on PushAdmin calls
 *   LOCKERS_PUSH_CONCURRENCY     max concurrent sends per backend (default 32)
 *   LOCKERS_PUSH_WORKER_ENABLED  run the queue-drain loop on this replica (default true; set false
 *                                on API-only replicas so exactly one worker drains the shared queue)
 *   LOCKERS_DB_URL               Postgres JDBC URL; unset => in-memory (dev only, not durable)
 *   LOCKERS_MAX_LOCKER_BYTES     max serialized locker payload (default 1 MiB)
 *   LOCKERS_ROOM_WRITES_PER_SEC  per-room sustained write rate (default 50; <=0 disables)
 *   LOCKERS_ROOM_WRITE_BURST     per-room burst allowance (default 100)
 *   LOCKERS_SESSION_CACHE_SIZE   room->sessions cache entries (default 1000)
 *   LOCKERS_SHARD_MULTIPLIER     shard count = cpuCores * this (default 4)
 *   APNS_TEAM_ID / APNS_KEY_ID / APNS_KEY_PATH / APNS_ENVIRONMENT
 *   APNS_TOPIC                   push topic (default com.latenighthack.lockers)
 *   FCM_CREDENTIALS_PATH         service-account JSON for FCM (unset => FCM off)
 *   WEBPUSH_VAPID_PUBLIC_KEY / WEBPUSH_VAPID_PRIVATE_KEY   base64url VAPID key pair
 *   WEBPUSH_SUBJECT              VAPID `sub` claim (mailto: or origin URL)
 */
data class LockersConfig(
    val httpPort: Int,
    val adminPort: Int,
    val databaseUrl: String?,
    val maxLockerPayloadBytes: Int,
    val roomWritesPerSecond: Int,
    val roomWriteBurst: Int,
    val sessionCacheSize: Long,
    val shardMultiplier: Int,
    val apns: ApnsConfig,
    val fcm: FcmConfig,
    val webPush: WebPushConfig,
    val pushSendConcurrency: Int,
    val pushWorkerEnabled: Boolean,
    val adminToken: String?,
) {
    val shardCount: Int get() = (Runtime.getRuntime().availableProcessors() * shardMultiplier).coerceAtLeast(1)

    companion object {
        fun fromEnv(env: (String) -> String? = System::getenv): LockersConfig {
            fun int(name: String, default: Int) = env(name)?.trim()?.toIntOrNull() ?: default
            fun long(name: String, default: Long) = env(name)?.trim()?.toLongOrNull() ?: default
            fun bool(name: String, default: Boolean) = env(name)?.trim()?.toBooleanStrictOrNull() ?: default
            return LockersConfig(
                httpPort = int("LOCKERS_HTTP_PORT", 8080),
                adminPort = int("LOCKERS_ADMIN_PORT", 8081),
                databaseUrl = env("LOCKERS_DB_URL")?.takeIf { it.isNotBlank() },
                maxLockerPayloadBytes = int("LOCKERS_MAX_LOCKER_BYTES", 1 * 1024 * 1024),
                roomWritesPerSecond = int("LOCKERS_ROOM_WRITES_PER_SEC", 50),
                roomWriteBurst = int("LOCKERS_ROOM_WRITE_BURST", 100),
                sessionCacheSize = long("LOCKERS_SESSION_CACHE_SIZE", 1000),
                shardMultiplier = int("LOCKERS_SHARD_MULTIPLIER", 4),
                pushSendConcurrency = int("LOCKERS_PUSH_CONCURRENCY", 32),
                pushWorkerEnabled = bool("LOCKERS_PUSH_WORKER_ENABLED", true),
                adminToken = env("LOCKERS_ADMIN_TOKEN")?.takeIf { it.isNotBlank() },
                apns = ApnsConfig(
                    teamId = env("APNS_TEAM_ID"),
                    keyId = env("APNS_KEY_ID"),
                    keyPath = env("APNS_KEY_PATH"),
                    environment = env("APNS_ENVIRONMENT") ?: "development",
                    topic = env("APNS_TOPIC") ?: "com.latenighthack.lockers",
                ),
                fcm = FcmConfig(
                    credentialsPath = env("FCM_CREDENTIALS_PATH"),
                ),
                webPush = WebPushConfig(
                    vapidPublicKey = env("WEBPUSH_VAPID_PUBLIC_KEY"),
                    vapidPrivateKey = env("WEBPUSH_VAPID_PRIVATE_KEY"),
                    subject = env("WEBPUSH_SUBJECT"),
                ),
            )
        }

        /** All-defaults config for tests and local embedding (no environment reads). */
        fun defaults(): LockersConfig = fromEnv { null }
    }
}
