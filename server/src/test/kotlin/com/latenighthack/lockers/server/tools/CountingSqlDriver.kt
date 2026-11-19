package com.latenighthack.lockers.server.tools

import com.latenighthack.ktstore.ManagedSqlDriver

/** Counts the advisory lock statements a JDBC driver actually issues (one per lockKey transaction). */
class CountingSqlDriver(private val inner: ManagedSqlDriver, private val record: suspend () -> Unit) :
    ManagedSqlDriver by inner {
    override suspend fun <T> transaction(block: suspend () -> T): T = inner.transaction(block)
    override suspend fun <T> transaction(lockKey: String, block: suspend () -> T): T {
        record()
        return inner.transaction(lockKey, block)
    }
}
