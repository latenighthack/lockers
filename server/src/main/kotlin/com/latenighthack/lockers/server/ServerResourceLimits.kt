package com.latenighthack.lockers.server

/** Finite operator budgets. Security tombstones consume reserved identity capacity permanently. */
data class ServerResourceLimits(
    val maxSessions: Int = 100_000,
    val maxReservedSessionIds: Int = 1_000_000,
    val maxOutstandingProofs: Int = 1_000_000,
    val maxOutstandingProofsPerSession: Int = 4096,
    val maxInboxEvents: Int = 1_000_000,
    val maxInboxEventsPerSession: Int = 10_000,
    val maxInboxBytes: Long = 1024L * 1024 * 1024,
    val maxInboxBytesPerSession: Long = 64L * 1024 * 1024,
    val maxInboxReceipts: Int = 10_000_000,
    val maxInboxReceiptsPerSession: Int = 1_000_000,
    val maxRoomClaims: Int = 1_000_000,
    val maxLockers: Int = 1_000_000,
    val maxLockersPerRoom: Int = 100_000,
    val maxLocks: Int = 1_000_000,
    val maxLocksPerRoom: Int = 10_000,
    val maxSubscriptions: Int = 1_000_000,
    val maxSubscriptionsPerSession: Int = 10_000,
    val maxSubscriptionsPerRoom: Int = 1024,
    val maxSnapshotLockers: Int = 10_000,
    val maxSnapshotBytes: Int = 64 * 1024 * 1024,
    val maxSnapshotLeases: Int = 128,
    val maxSnapshotLeasesPerRoom: Int = 4,
    val maxSnapshotRetainedBytes: Long = 256L * 1024 * 1024,
    val snapshotLeaseMillis: Long = 60_000,
    val maxLegacySnapshotLockers: Int = 1024,
    val globalCpuUnitsPerSecond: Int = 2000,
    val globalCpuBurst: Int = 4000,
    val globalReadsPerSecond: Int = 1000,
    val globalReadBurst: Int = 2000,
    val roomReadsPerSecond: Int = 100,
    val roomReadBurst: Int = 200,
    val maxTrackedReadRooms: Int = 4096,
) {
    init {
        require(maxSessions > 0 && maxReservedSessionIds >= maxSessions)
        require(maxOutstandingProofs > 0 && maxOutstandingProofsPerSession in 1..maxOutstandingProofs)
        require(maxInboxBytes > 0 && maxInboxBytesPerSession in 1..maxInboxBytes)
        require(maxInboxEvents > 0 && maxInboxEventsPerSession in 1..maxInboxEvents)
        require(maxInboxReceipts > 0 && maxInboxReceiptsPerSession in 1..maxInboxReceipts)
        require(maxRoomClaims > 0)
        require(maxLocks > 0 && maxLocksPerRoom in 1..maxLocks)
        require(maxSubscriptions > 0 && maxSubscriptionsPerSession in 1..maxSubscriptions && maxSubscriptionsPerRoom in 1..1024)
        require(maxLockers > 0 && maxLockersPerRoom in 1..maxLockers)
        require(maxSnapshotLeasesPerRoom > 0 && maxSnapshotRetainedBytes > 0)
        require(maxSnapshotLockers > 0 && maxSnapshotBytes > 0 && maxSnapshotLeases > 0)
        require(globalCpuUnitsPerSecond > 0 && globalCpuBurst > 0)
        require(globalReadsPerSecond > 0 && globalReadBurst > 0 && roomReadsPerSecond > 0 && roomReadBurst > 0 && maxTrackedReadRooms > 0)
        require(snapshotLeaseMillis in 1..3_600_000 && maxLegacySnapshotLockers > 0)
    }
    companion object {
        fun fromEnv(env: (String) -> String?): ServerResourceLimits {
            fun limit(name: String, fallback: Int) = env(name)?.let {
                requireNotNull(it.trim().toIntOrNull()) { "Invalid $name" }
            } ?: fallback
            fun byteLimit(name: String, fallback: Long) = env(name)?.let {
                requireNotNull(it.trim().toLongOrNull()) { "Invalid $name" }
            } ?: fallback
            return ServerResourceLimits(
                maxSessions = limit("LOCKERS_MAX_SESSIONS", 100_000),
                maxReservedSessionIds = limit("LOCKERS_MAX_RESERVED_SESSION_IDS", 1_000_000),
                maxOutstandingProofs = limit("LOCKERS_MAX_PROOFS", 1_000_000),
                maxOutstandingProofsPerSession = limit("LOCKERS_MAX_SESSION_PROOFS", 4096),
                maxInboxEvents = limit("LOCKERS_MAX_INBOX_EVENTS", 1_000_000),
                maxInboxEventsPerSession = limit("LOCKERS_MAX_SESSION_INBOX_EVENTS", 10_000),
                maxInboxBytes = byteLimit("LOCKERS_MAX_INBOX_BYTES", 1024L * 1024 * 1024),
                maxInboxBytesPerSession = byteLimit("LOCKERS_MAX_SESSION_INBOX_BYTES", 64L * 1024 * 1024),
                maxInboxReceipts = limit("LOCKERS_MAX_INBOX_RECEIPTS", 10_000_000),
                maxInboxReceiptsPerSession = limit("LOCKERS_MAX_SESSION_INBOX_RECEIPTS", 1_000_000),
                maxRoomClaims = limit("LOCKERS_MAX_ROOM_CLAIMS", 1_000_000),
                maxLockers = limit("LOCKERS_MAX_STORED_LOCKERS", 1_000_000),
                maxLockersPerRoom = limit("LOCKERS_MAX_ROOM_LOCKERS", 100_000),
                maxLocks = limit("LOCKERS_MAX_LOCK_HISTORY", 1_000_000),
                maxLocksPerRoom = limit("LOCKERS_MAX_ROOM_LOCK_HISTORY", 10_000),
                maxSubscriptions = limit("LOCKERS_MAX_SUBSCRIPTIONS", 1_000_000),
                maxSubscriptionsPerSession = limit("LOCKERS_MAX_SESSION_SUBSCRIPTIONS", 10_000),
                maxSubscriptionsPerRoom = limit("LOCKERS_MAX_ROOM_SUBSCRIPTIONS", 1024),
                maxSnapshotLockers = limit("LOCKERS_MAX_SNAPSHOT_LOCKERS", 10_000),
                maxSnapshotBytes = limit("LOCKERS_MAX_SNAPSHOT_BYTES", 64 * 1024 * 1024),
                maxSnapshotLeases = limit("LOCKERS_MAX_SNAPSHOT_LEASES", 128),
                maxSnapshotLeasesPerRoom = limit("LOCKERS_MAX_ROOM_SNAPSHOT_LEASES", 4),
                maxSnapshotRetainedBytes = limit("LOCKERS_MAX_RETAINED_SNAPSHOT_BYTES", 256 * 1024 * 1024).toLong(),
                snapshotLeaseMillis = limit("LOCKERS_SNAPSHOT_LEASE_MS", 60_000).toLong(),
                maxLegacySnapshotLockers = limit("LOCKERS_MAX_LEGACY_SNAPSHOT_LOCKERS", 1024),
                globalCpuUnitsPerSecond = limit("LOCKERS_CPU_UNITS_PER_SEC", 2000),
                globalCpuBurst = limit("LOCKERS_CPU_BURST", 4000),
                globalReadsPerSecond = limit("LOCKERS_READS_PER_SEC", 1000),
                globalReadBurst = limit("LOCKERS_READ_BURST", 2000),
                roomReadsPerSecond = limit("LOCKERS_ROOM_READS_PER_SEC", 100),
                roomReadBurst = limit("LOCKERS_ROOM_READ_BURST", 200),
                maxTrackedReadRooms = limit("LOCKERS_MAX_READ_ROOM_BUCKETS", 4096),
            )
        }
    }
}

internal class ResourceLimitException(message: String) : IllegalStateException(message)
