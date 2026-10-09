package com.latenighthack.lockers.server.tools

import com.latenighthack.ktstore.PostgresJdbcLimits
import java.sql.Connection
import java.sql.DriverManager

/** Opens finite PostgreSQL connections and rolls back allocation when initialization fails. */
internal fun openPostgresConnection(url: String, limits: PostgresJdbcLimits = PostgresJdbcLimits()): Connection {
    val connection = DriverManager.getConnection(limits.boundedUrl(url))
    try { limits.initialize(connection); return connection }
    catch (failure: Throwable) {
        try { connection.close() } catch (cleanup: Throwable) { failure.addSuppressed(cleanup) }
        throw failure
    }
}
