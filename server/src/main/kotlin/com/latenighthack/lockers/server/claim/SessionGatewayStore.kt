package com.latenighthack.lockers.server.claim

import com.latenighthack.lockers.common.v1.SessionId

/** The `session_gateway` row for a live session: which node holds its WebSocket, and where. */
data class SessionGatewayRow(val nodeId: String, val nodeAddr: String)

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

    /** Batched TTL extension for every unexpired row owned by [nodeId]; returns the renewed set. */
    suspend fun renewAll(nodeId: String, ttlMs: Long): Set<SessionId>

    /** Deletes the row only if [nodeId] still holds it (a moved session's new row survives). */
    suspend fun delete(sessionId: SessionId, nodeId: String)

    /** Graceful drain: deletes every row held by [nodeId]. */
    suspend fun releaseAll(nodeId: String)

    /** The live (unexpired) row, or null — null means offline (push-queue path). */
    suspend fun lookup(sessionId: SessionId): SessionGatewayRow?
}

/** Production [SessionGatewayStore] over the shared Postgres, plain JDBC on a [ClaimJdbcPool]. */
class JdbcSessionGatewayStore(private val pool: ClaimJdbcPool) : SessionGatewayStore {
    override suspend fun prepare() {
        pool.withConnection { conn ->
            conn.createStatement().use { st ->
                st.execute(TABLE_DDL)
                st.execute(INDEX_DDL)
            }
        }
    }

    override suspend fun upsert(sessionId: SessionId, nodeId: String, nodeAddr: String, ttlMs: Long) {
        pool.withConnection { conn ->
            conn.prepareStatement(UPSERT_SQL).use { st ->
                st.setBytes(1, sessionId.rawValue)
                st.setString(2, nodeId)
                st.setString(3, nodeAddr)
                st.setDouble(4, ttlMs.toDouble())
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
                    if (rs.next()) SessionGatewayRow(rs.getString("node_id"), rs.getString("node_addr")) else null
                }
            }
        }

    companion object {
        const val TABLE_DDL = """
            CREATE TABLE IF NOT EXISTS session_gateway (
                session_id  BYTEA PRIMARY KEY,
                node_id     TEXT        NOT NULL,
                node_addr   TEXT        NOT NULL,
                expires_at  TIMESTAMPTZ NOT NULL
            )
        """

        const val INDEX_DDL = """
            CREATE INDEX IF NOT EXISTS session_gateway_node ON session_gateway (node_id)
        """

        private const val UPSERT_SQL = """
            INSERT INTO session_gateway (session_id, node_id, node_addr, expires_at)
            VALUES (?, ?, ?, now() + (? * interval '1 millisecond'))
            ON CONFLICT (session_id) DO UPDATE
               SET node_id = EXCLUDED.node_id,
                   node_addr = EXCLUDED.node_addr,
                   expires_at = EXCLUDED.expires_at
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
            "SELECT node_id, node_addr FROM session_gateway WHERE session_id = ? AND expires_at >= now()"
    }
}
