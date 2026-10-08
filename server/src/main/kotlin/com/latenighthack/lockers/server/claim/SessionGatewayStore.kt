package com.latenighthack.lockers.server.claim

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
class JdbcSessionGatewayStore(private val pool: ClaimJdbcPool) : SessionGatewayStore {
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
