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
 * Horizontal ring-sharding configuration (the VM/bare-metal + Postgres blueprint). All fields
 * default to the disabled/monolith values, so an unset environment yields a single-node process
 * that behaves exactly as before. [nodeId] + [peers] together enable cluster mode
 * ([LockersConfig.clusterEnabled]); the counts/vnodes size the two rings.
 *
 * Parsing of [peers] into a validated topology and of [keyspaceShardCounts] lives in
 * `:sharding-core` / the `cluster` package — this holds only the raw strings so [LockersConfig]
 * stays free of a `:sharding-core` dependency and fails fast at boot in `Main`, not here.
 */
data class ShardingConfig(
    val nodeId: String?,
    val advertiseAddr: String?,
    val peers: String?,
    val shardCountDefault: Int,
    val keyspaceShardCounts: String?,
    val ringVnodes: Int,
    val sessionShardCount: Int,
)

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
 *   LOCKERS_NODE_ID              this node's logical ring identity (default: unset => monolith)
 *   LOCKERS_ADVERTISE_ADDR       peer-reachable host:port for east-west RPC
 *   LOCKERS_PEERS                comma-separated node list; presence enables cluster mode
 *   LOCKERS_SHARD_COUNT_DEFAULT  room-ring global shard count (default 256)
 *   LOCKERS_KEYSPACE_SHARD_COUNTS  per-keyspace overrides, e.g. "1=512,30=128"
 *   LOCKERS_RING_VNODES          consistent-hash virtual nodes per node (default 128)
 *   LOCKERS_SESSION_SHARD_COUNT  session/gateway-ring shard count (default 256)
 *   LOCKERS_REQUIRE_DB           fail fast at boot if LOCKERS_DB_URL is unset (default false)
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
    val sharding: ShardingConfig,
    val requireDb: Boolean,
) {
    val shardCount: Int get() = (Runtime.getRuntime().availableProcessors() * shardMultiplier).coerceAtLeast(1)

    /**
     * Whether horizontal ring-sharding is enabled. Requires both a peer list and this node's
     * identity; when either is absent the process runs as the single-node monolith (the exact
     * pre-sharding behavior). This is intentionally a two-signal gate so a stray env var never
     * silently flips a monolith into a mis-configured cluster.
     */
    val clusterEnabled: Boolean
        get() = !sharding.peers.isNullOrBlank() && !sharding.nodeId.isNullOrBlank()

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
                sharding = ShardingConfig(
                    nodeId = env("LOCKERS_NODE_ID")?.takeIf { it.isNotBlank() },
                    advertiseAddr = env("LOCKERS_ADVERTISE_ADDR")?.takeIf { it.isNotBlank() },
                    peers = env("LOCKERS_PEERS")?.takeIf { it.isNotBlank() },
                    shardCountDefault = int("LOCKERS_SHARD_COUNT_DEFAULT", 256),
                    keyspaceShardCounts = env("LOCKERS_KEYSPACE_SHARD_COUNTS")?.takeIf { it.isNotBlank() },
                    ringVnodes = int("LOCKERS_RING_VNODES", 128),
                    sessionShardCount = int("LOCKERS_SESSION_SHARD_COUNT", 256),
                ),
                requireDb = bool("LOCKERS_REQUIRE_DB", false),
            )
        }

        /** All-defaults config for tests and local embedding (no environment reads). */
        fun defaults(): LockersConfig = fromEnv { null }
    }
}
