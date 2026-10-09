package com.latenighthack.lockers.server

/** Finite operator budgets. Security tombstones consume reserved identity capacity permanently. */
data class ServerResourceLimits(
    val maxSessions: Int = 100_000,
    val maxReservedSessionIds: Int = 1_000_000,
    val maxInboxEvents: Int = 1_000_000,
    val maxInboxEventsPerSession: Int = 10_000,
    val maxRoomClaims: Int = 1_000_000,
    val maxLockers: Int = 1_000_000,
    val maxLockersPerRoom: Int = 100_000,
    val maxSnapshotLockers: Int = 10_000,
    val maxSnapshotBytes: Int = 64 * 1024 * 1024,
    val maxSnapshotLeases: Int = 128,
    val snapshotLeaseMillis: Long = 60_000,
    val maxLegacySnapshotLockers: Int = 1024,
) {
    init {
        require(maxSessions > 0 && maxReservedSessionIds >= maxSessions)
        require(maxInboxEvents > 0 && maxInboxEventsPerSession in 1..maxInboxEvents)
        require(maxRoomClaims > 0)
        require(maxLockers > 0 && maxLockersPerRoom in 1..maxLockers)
        require(maxSnapshotLockers > 0 && maxSnapshotBytes > 0 && maxSnapshotLeases > 0)
        require(snapshotLeaseMillis in 1..3_600_000 && maxLegacySnapshotLockers > 0)
    }
    companion object {
        fun fromEnv(env: (String) -> String?): ServerResourceLimits {
            fun limit(name: String, fallback: Int) = env(name)?.let {
                requireNotNull(it.trim().toIntOrNull()) { "Invalid $name" }
            } ?: fallback
            return ServerResourceLimits(
                maxSessions = limit("LOCKERS_MAX_SESSIONS", 100_000),
                maxReservedSessionIds = limit("LOCKERS_MAX_RESERVED_SESSION_IDS", 1_000_000),
                maxInboxEvents = limit("LOCKERS_MAX_INBOX_EVENTS", 1_000_000),
                maxInboxEventsPerSession = limit("LOCKERS_MAX_SESSION_INBOX_EVENTS", 10_000),
                maxRoomClaims = limit("LOCKERS_MAX_ROOM_CLAIMS", 1_000_000),
                maxLockers = limit("LOCKERS_MAX_STORED_LOCKERS", 1_000_000),
                maxLockersPerRoom = limit("LOCKERS_MAX_ROOM_LOCKERS", 100_000),
                maxSnapshotLockers = limit("LOCKERS_MAX_SNAPSHOT_LOCKERS", 10_000),
                maxSnapshotBytes = limit("LOCKERS_MAX_SNAPSHOT_BYTES", 64 * 1024 * 1024),
                maxSnapshotLeases = limit("LOCKERS_MAX_SNAPSHOT_LEASES", 128),
                snapshotLeaseMillis = limit("LOCKERS_SNAPSHOT_LEASE_MS", 60_000).toLong(),
                maxLegacySnapshotLockers = limit("LOCKERS_MAX_LEGACY_SNAPSHOT_LOCKERS", 1024),
            )
        }
    }
}

internal class ResourceLimitException(message: String) : IllegalStateException(message)
