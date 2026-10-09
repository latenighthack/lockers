package com.latenighthack.lockers.server.claim

import com.latenighthack.lockers.server.tools.QueueSnapshot

import com.latenighthack.lockers.common.v1.SessionId

/** The `session_gateway` row for a live session: which node holds its WebSocket, and where. */
data class SessionGatewayRow(val nodeId: String, val nodeAddr: String, val attachmentId: String = "")

/**
 * Fan-out discovery registry: one TTL-renewed row per live WebSocket session, written by the node
 * that holds the socket. Unlike [RoomClaimStore] there is no fence and no steal condition — the
 * upsert is unconditional because a reconnect legitimately moves a session to another node. A
 * missing or expired row means "offline"; delivery falls back to the push-queue path.
 */
interface SessionGatewayStore {
    val supportsInboxPresence: Boolean get() = false
    /** Optional database-wide view. Offline accumulation is expected, not stalled live work. */
    suspend fun inboxPresenceSnapshot(): Map<String, QueueSnapshot>? = null
    suspend fun prepare()

    /** Registers/moves the session to [nodeId] unconditionally and (re)starts its TTL. */
    suspend fun upsert(sessionId: SessionId, nodeId: String, nodeAddr: String, ttlMs: Long)

    /** An attachment incarnation prevents a late close from deleting a replacement socket. */
    suspend fun upsert(sessionId: SessionId, nodeId: String, nodeAddr: String, ttlMs: Long, attachmentId: String) {
        throw UnsupportedOperationException("SessionGatewayStore requires attachment fencing")
    }

    suspend fun delete(sessionId: SessionId, nodeId: String, attachmentId: String) {
        throw UnsupportedOperationException("SessionGatewayStore requires attachment fencing")
    }

    /** Batched TTL extension for every unexpired row owned by [nodeId]; returns the renewed set. */
    suspend fun renewAll(nodeId: String, ttlMs: Long): Set<SessionId>

    /** Deletes the row only if [nodeId] still holds it (a moved session's new row survives). */
    suspend fun delete(sessionId: SessionId, nodeId: String)

    /** Graceful drain: deletes every row held by [nodeId]. */
    suspend fun releaseAll(nodeId: String)

    /** The live (unexpired) row, or null — null means offline (push-queue path). */
    suspend fun lookup(sessionId: SessionId): SessionGatewayRow?
    suspend fun lookupMany(sessionIds: List<SessionId>): Map<SessionId, SessionGatewayRow> =
        sessionIds.mapNotNull { id -> lookup(id)?.let { id to it } }.toMap()
}

/** Production [SessionGatewayStore] over the shared Postgres, plain JDBC on a [ClaimJdbcPool]. */
private const val PRESENCE_TIMEOUT_MILLIS = 4_000L
private const val ORDERED_LONG_BYTES = 8
private const val BITS_PER_BYTE = 8
private const val UNSIGNED_BYTE_MASK = 255L

    private fun decodeEnqueued(encoded: ByteArray): Long {
        require(encoded.size == ORDERED_LONG_BYTES)
        return encoded.fold(0L) { value, byte ->
            (value shl BITS_PER_BYTE) or (byte.toLong() and UNSIGNED_BYTE_MASK)
        } xor Long.MIN_VALUE
    }

class JdbcSessionGatewayStore(private val pool: ClaimJdbcPool, private val inboxPool: ClaimJdbcPool? =
    null) : SessionGatewayStore {
    override val supportsInboxPresence: Boolean get() = inboxPool != null
    // Restore connection transaction state and propagate cancellation and database failures.
    @Suppress("TooGenericExceptionCaught")
    override suspend fun inboxPresenceSnapshot(): Map<String, QueueSnapshot>? {
        val operations = inboxPool ?: return null
        return kotlinx.coroutines.withTimeout(PRESENCE_TIMEOUT_MILLIS) { operations.withConnection { conn ->
            val autoCommit = conn.autoCommit
            val isolation = conn.transactionIsolation
            val readOnly = conn.isReadOnly
            conn.transactionIsolation = java.sql.Connection.TRANSACTION_REPEATABLE_READ
            conn.isReadOnly = true
            conn.autoCommit = false
            try {
                val zero = com.latenighthack.lockers.server.tools.EpochMillisCodec.encode(0)
                val counts = mutableMapOf<String, Pair<Long, Long>>()
                conn.prepareStatement(INBOX_COUNTS_SQL).use { stmt ->
                    stmt.queryTimeout = 1
                    stmt.setBytes(1, zero)
                    stmt.executeQuery().use { rows ->
                        while (rows.next()) counts[rows.getString("presence")] =
                            rows.getLong("depth") to rows.getLong("unknown_age")
                    }
                }
                val snapshot = listOf("online", "offline").associateWith { presence ->
                    val (depth, unknown) = counts[presence] ?: (0L to 0L)
                    val oldest = if (depth == unknown) null else {
                        val sql = if (presence == "online") INBOX_OLDEST_ONLINE_SQL else INBOX_OLDEST_OFFLINE_SQL
                        conn.prepareStatement(sql).use { stmt ->
                            stmt.queryTimeout = 1
                            stmt.setBytes(1, zero)
                            stmt.executeQuery().use { rows ->
                                if (!rows.next()) null else decodeEnqueued(rows.getBytes(1))
                            }
                        }
                    }
                    QueueSnapshot(depth, unknown, oldest)
                }
                conn.commit()
                snapshot
            } catch (error: Throwable) {
                runCatching { conn.rollback() }
                throw error
            } finally {
                conn.autoCommit = autoCommit
                conn.isReadOnly = readOnly
                conn.transactionIsolation = isolation
            }
        } }
    }
    override suspend fun prepare() {
        pool.withConnection { conn ->
            conn.createStatement().use { st ->
                st.execute(TABLE_DDL)
                st.execute("ALTER TABLE session_gateway ADD COLUMN IF NOT EXISTS attachment_id TEXT NOT NULL DEFAULT ''")
                st.execute(INDEX_DDL)
            }
        }
    }

    override suspend fun upsert(sessionId: SessionId, nodeId: String, nodeAddr: String, ttlMs: Long) =
        upsert(sessionId, nodeId, nodeAddr, ttlMs, "")

    override suspend fun upsert(sessionId: SessionId, nodeId: String, nodeAddr: String, ttlMs: Long, attachmentId: String) {
        pool.withConnection { conn ->
            conn.prepareStatement(UPSERT_SQL).use { st ->
                st.setBytes(1, sessionId.rawValue)
                st.setString(2, nodeId)
                st.setString(3, nodeAddr)
                st.setDouble(4, ttlMs.toDouble())
                st.setString(5, attachmentId)
                st.executeUpdate()
            }
        }
    }

    override suspend fun renewAll(nodeId: String, ttlMs: Long): Set<SessionId> =
        pool.withConnection { conn ->
            conn.prepareStatement(RENEW_SQL).use { st ->
                st.setDouble(1, ttlMs.toDouble())
                st.setString(2, nodeId)
                st.executeQuery().use { rs ->
                    buildSet {
                        while (rs.next()) add(SessionId(rawValue = rs.getBytes("session_id")))
                    }
                }
            }
        }

    override suspend fun delete(sessionId: SessionId, nodeId: String) {
        pool.withConnection { conn ->
            conn.prepareStatement(DELETE_SQL).use { st ->
                st.setBytes(1, sessionId.rawValue)
                st.setString(2, nodeId)
                st.executeUpdate()
            }
        }
    }

    override suspend fun delete(sessionId: SessionId, nodeId: String, attachmentId: String) {
        pool.withConnection { conn ->
            conn.prepareStatement("DELETE FROM session_gateway WHERE session_id = ? AND node_id = ? AND attachment_id = ?").use { st ->
                st.setBytes(1, sessionId.rawValue)
                st.setString(2, nodeId)
                st.setString(3, attachmentId)
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

    override suspend fun lookup(sessionId: SessionId): SessionGatewayRow? =
        pool.withConnection { conn ->
            conn.prepareStatement(LOOKUP_SQL).use { st ->
                st.setBytes(1, sessionId.rawValue)
                st.executeQuery().use { rs ->
                    if (rs.next()) SessionGatewayRow(rs.getString("node_id"), rs.getString("node_addr"), rs.getString("attachment_id")) else null
                }
            }
        }

    override suspend fun lookupMany(sessionIds: List<SessionId>): Map<SessionId, SessionGatewayRow> {
        if (sessionIds.isEmpty()) return emptyMap()
        return pool.withConnection { conn ->
            buildMap {
                for (ids in sessionIds.distinct().chunked(512)) {
                    val placeholders = ids.joinToString(",") { "?" }
                    conn.prepareStatement("SELECT session_id, node_id, node_addr, attachment_id FROM session_gateway WHERE session_id IN ($placeholders) AND expires_at >= now()").use { st ->
                        ids.forEachIndexed { index, id -> st.setBytes(index + 1, id.rawValue) }
                        st.executeQuery().use { rs ->
                            while (rs.next()) put(SessionId(rs.getBytes("session_id")), SessionGatewayRow(rs.getString("node_id"), rs.getString("node_addr"), rs.getString("attachment_id")))
                        }
                    }
                }
            }
        }
    }

    companion object {
        // These indexed columns are owned by SessionInboxStoreDefinitionV3. The
        // aggregate never selects or deserializes event bodies, and each statement
        // has a one-second database deadline. Repeatable read keeps the split and
        // ages consistent while gateways move, expire or ACKs delete inbox rows.
        private const val INBOX_COUNTS_SQL = """
            SELECT CASE WHEN g.session_id IS NULL THEN 'offline' ELSE 'online' END AS presence,
                   count(*) AS depth, count(*) FILTER (WHERE i.enqueued = ?) AS unknown_age
              FROM inbox i LEFT JOIN session_gateway g
                ON i.sessionRaw = g.session_id AND g.expires_at >= now()
             GROUP BY CASE WHEN g.session_id IS NULL THEN 'offline' ELSE 'online' END
        """
        private const val INBOX_OLDEST_ONLINE_SQL = """
            SELECT i.enqueued FROM inbox i WHERE i.enqueued > ?
               AND EXISTS (SELECT 1 FROM session_gateway g WHERE g.session_id = i.sessionRaw AND g.expires_at >= now())
             ORDER BY i.enqueued LIMIT 1
        """
        private const val INBOX_OLDEST_OFFLINE_SQL = """
            SELECT i.enqueued FROM inbox i WHERE i.enqueued > ?
               AND NOT EXISTS (SELECT 1 FROM session_gateway g WHERE g.session_id =
                   i.sessionRaw AND g.expires_at >= now())
             ORDER BY i.enqueued LIMIT 1
        """
        const val TABLE_DDL = """
            CREATE TABLE IF NOT EXISTS session_gateway (
                session_id  BYTEA PRIMARY KEY,
                node_id     TEXT        NOT NULL,
                node_addr   TEXT        NOT NULL,
                expires_at  TIMESTAMPTZ NOT NULL,
                attachment_id TEXT NOT NULL DEFAULT ''
            )
        """

        const val INDEX_DDL = """
            CREATE INDEX IF NOT EXISTS session_gateway_node ON session_gateway (node_id)
        """

        private const val UPSERT_SQL = """
            INSERT INTO session_gateway (session_id, node_id, node_addr, expires_at, attachment_id)
            VALUES (?, ?, ?, now() + (? * interval '1 millisecond'), ?)
            ON CONFLICT (session_id) DO UPDATE
               SET node_id = EXCLUDED.node_id,
                   node_addr = EXCLUDED.node_addr,
                   expires_at = EXCLUDED.expires_at,
                   attachment_id = EXCLUDED.attachment_id
        """

        private const val RENEW_SQL = """
            UPDATE session_gateway SET expires_at = now() + (? * interval '1 millisecond')
             WHERE node_id = ? AND expires_at >= now()
            RETURNING session_id
        """

        private const val DELETE_SQL =
            "DELETE FROM session_gateway WHERE session_id = ? AND node_id = ?"

        private const val RELEASE_ALL_SQL =
            "DELETE FROM session_gateway WHERE node_id = ?"

        private const val LOOKUP_SQL =
            "SELECT node_id, node_addr, attachment_id FROM session_gateway WHERE session_id = ? AND expires_at >= now()"
    }
}
