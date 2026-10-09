package com.latenighthack.lockers.server.claim

import com.latenighthack.lockers.common.v1.RoomId

/** The current `room_claim` row for a room: who owns it, where to redirect, and at which epoch. */
data class RoomClaimRow(val nodeId: String, val nodeAddr: String, val epoch: Long)

/**
 * Room-granularity ownership claims (see `docs/design/claim-ownership.md`): one TTL-renewed row per
 * active room is both the router and the fence. All expiry decisions use the backing store's clock
 * (`now()` in Postgres) so there is a single clock authority; node clock skew never changes an
 * ownership decision. Implementations: [JdbcRoomClaimStore] (production, shared Postgres) and
 * [InMemoryRoomClaimStore] (tests / embedding); a Redis impl can drop in behind this interface if
 * claim QPS ever pressures Postgres.
 */
interface RoomClaimStore {
    fun mutationFence(roomId: RoomId, nodeId: String, epoch: Long): com.latenighthack.lockers.server.services.room.v1.RoomMutationFence =
        com.latenighthack.lockers.server.services.room.v1.ClaimMutationFence(roomId, nodeId, epoch)
    /** Idempotently creates the backing table/index (the app-table `createStores()` precedent). */
    suspend fun prepare()

    /**
     * Atomic insert-or-steal-if-expired-or-renew-own, in one round trip. Always returns the
     * current owner's row: `nodeId == [nodeId]` means this node owns the room at `epoch`; any other
     * value is a valid owner to redirect to. The epoch advances after expiry, release or a steal, including a reclaim by
     * the same process. An unexpired self-renew retains its epoch. Released rows remain
     * present so no previous fencing token can become current again.
     */
    suspend fun claim(roomId: RoomId, nodeId: String, nodeAddr: String, ttlMs: Long): RoomClaimRow

    /**
     * Extends every unexpired claim held by [nodeId] in one statement and returns exactly the
     * still-owned set. Rooms missing versus the caller's local view were lost (expired + stolen)
     * and must be demoted. This is the entire steady-state coordination cost of claim mode.
     */
    suspend fun renewAll(nodeId: String, ttlMs: Long): Set<RoomId>

    /** Expires the claim, preserving its epoch, only if [nodeId] still owns it (release-after-steal is a no-op). */
    suspend fun release(roomId: RoomId, nodeId: String)

    /** Graceful drain: expires every claim held by [nodeId] so successors need not wait out a TTL. */
    suspend fun releaseAll(nodeId: String)

    /** The raw row, including an expired one (the caller decides what expiry means), or null. */
    suspend fun lookup(roomId: RoomId): RoomClaimRow?

    /** Cheap liveness probe for `/readyz`. */
    suspend fun ping()
}

/** Production [RoomClaimStore] over the shared Postgres, plain JDBC on a [ClaimJdbcPool]. */
class RoomClaimCapacityExceeded : IllegalStateException("Permanent room claim history capacity exceeded")

class JdbcRoomClaimStore(private val pool: ClaimJdbcPool, private val maxRoomClaims: Long = 1_000_000) : RoomClaimStore {
    init { require(maxRoomClaims > 0) }
    override suspend fun prepare() {
        pool.withConnection { conn ->
            conn.createStatement().use { st ->
                st.execute(TABLE_DDL)
                st.execute(INDEX_DDL)
                st.execute("CREATE TABLE IF NOT EXISTS room_claim_capacity (singleton INTEGER PRIMARY KEY CHECK (singleton = 1), reserved BIGINT NOT NULL CHECK (reserved >= 0))")
                st.execute("INSERT INTO room_claim_capacity (singleton, reserved) SELECT 1, COUNT(*) FROM room_claim ON CONFLICT (singleton) DO NOTHING")
            }
        }
    }

    override suspend fun claim(roomId: RoomId, nodeId: String, nodeAddr: String, ttlMs: Long): RoomClaimRow {
        require(roomId.rawValue.size in 1..128 && nodeId.isNotBlank() && nodeAddr.isNotBlank() && ttlMs > 0)
        return pool.withConnection { conn ->
            conn.autoCommit = false
            try {
                fun lookup(): RoomClaimRow? = conn.prepareStatement(LOOKUP_SQL).use { st ->
                    st.setBytes(1, roomId.rawValue)
                    st.executeQuery().use { rs -> if (rs.next()) rs.toClaimRow() else null }
                }
                if (lookup() == null) {
                    val count = conn.createStatement().use { st ->
                        st.executeQuery("SELECT reserved FROM room_claim_capacity WHERE singleton = 1 FOR UPDATE").use { rs ->
                            check(rs.next()) { "Room claim capacity must be prepared before admission" }
                            rs.getLong(1)
                        }
                    }
                    // A competing admission may have installed this identity while we waited.
                    if (lookup() == null) {
                        if (count >= maxRoomClaims) throw RoomClaimCapacityExceeded()
                        conn.createStatement().use { it.executeUpdate("UPDATE room_claim_capacity SET reserved = reserved + 1 WHERE singleton = 1") }
                    }
                }
                val row = conn.prepareStatement(CLAIM_SQL).use { st ->
                    st.setBytes(1, roomId.rawValue)
                    st.setString(2, nodeId)
                    st.setString(3, nodeAddr)
                    st.setDouble(4, ttlMs.toDouble())
                    st.executeQuery().use { rs -> if (rs.next()) rs.toClaimRow() else null }
                } ?: requireNotNull(lookup()) { "Room claim disappeared during admission" }
                conn.commit()
                row
            } catch (failure: Throwable) {
                try { conn.rollback() } catch (cleanup: Throwable) { failure.addSuppressed(cleanup) }
                throw failure
            } finally { conn.autoCommit = true }
        }
    }

    override suspend fun renewAll(nodeId: String, ttlMs: Long): Set<RoomId> =
        pool.withConnection { conn ->
            conn.prepareStatement(RENEW_SQL).use { st ->
                st.setDouble(1, ttlMs.toDouble())
                st.setString(2, nodeId)
                st.executeQuery().use { rs ->
                    buildSet {
                        while (rs.next()) add(RoomId(rawValue = rs.getBytes("room_id")))
                    }
                }
            }
        }

    override suspend fun release(roomId: RoomId, nodeId: String) {
        pool.withConnection { conn ->
            conn.prepareStatement(RELEASE_SQL).use { st ->
                st.setBytes(1, roomId.rawValue)
                st.setString(2, nodeId)
                st.executeUpdate()
            }
        }
    }

    override suspend fun releaseAll(nodeId: String) {
        pool.withConnection { conn ->
            conn.prepareStatement(RELEASE_ALL_SQL).use { st ->
                st.setString(1, nodeId)
                st.executeUpdate()
            }
        }
    }

    override suspend fun lookup(roomId: RoomId): RoomClaimRow? =
        pool.withConnection { conn ->
            conn.prepareStatement(LOOKUP_SQL).use { st ->
                st.setBytes(1, roomId.rawValue)
                st.executeQuery().use { rs -> if (rs.next()) rs.toClaimRow() else null }
            }
        }

    override suspend fun ping() {
        pool.withConnection { conn -> conn.createStatement().use { it.execute("SELECT 1") } }
    }

    private fun java.sql.ResultSet.toClaimRow() = RoomClaimRow(
        nodeId = getString("node_id"),
        nodeAddr = getString("node_addr"),
        epoch = getLong("epoch"),
    )

    companion object {

        const val TABLE_DDL = """
            CREATE TABLE IF NOT EXISTS room_claim (
                room_id     BYTEA PRIMARY KEY,
                node_id     TEXT        NOT NULL,
                node_addr   TEXT        NOT NULL,
                epoch       BIGINT      NOT NULL DEFAULT 1,
                expires_at  TIMESTAMPTZ NOT NULL
            )
        """

        const val INDEX_DDL = """
            CREATE INDEX IF NOT EXISTS room_claim_node ON room_claim (node_id)
        """

        // Steal only if expired, or re-claim our own row; epoch bumps only when ownership actually
        // changes hands (the CASE), keeping it a takeover counter as well as a fence.
        private const val CLAIM_SQL = """
            INSERT INTO room_claim (room_id, node_id, node_addr, epoch, expires_at)
            VALUES (?, ?, ?, 1, now() + (? * interval '1 millisecond'))
            ON CONFLICT (room_id) DO UPDATE
               SET node_id    = EXCLUDED.node_id,
                   node_addr  = EXCLUDED.node_addr,
                   epoch      = CASE WHEN room_claim.node_id = EXCLUDED.node_id AND room_claim.expires_at >= now()
                                     THEN room_claim.epoch
                                     ELSE room_claim.epoch + 1 END,
                   expires_at = EXCLUDED.expires_at
             WHERE room_claim.expires_at < now()
                OR room_claim.node_id = EXCLUDED.node_id
            RETURNING node_id, node_addr, epoch
        """

        private const val RENEW_SQL = """
            UPDATE room_claim SET expires_at = now() + (? * interval '1 millisecond')
             WHERE node_id = ? AND expires_at >= now()
            RETURNING room_id
        """

        private const val RELEASE_SQL =
            "UPDATE room_claim SET expires_at = '-infinity'::timestamptz WHERE room_id = ? AND node_id = ?"

        private const val RELEASE_ALL_SQL =
            "UPDATE room_claim SET expires_at = '-infinity'::timestamptz WHERE node_id = ?"

        private const val LOOKUP_SQL =
            "SELECT node_id, node_addr, epoch FROM room_claim WHERE room_id = ?"
    }
}
