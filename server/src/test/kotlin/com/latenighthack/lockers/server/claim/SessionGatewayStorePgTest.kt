package com.latenighthack.lockers.server.claim

import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.BeforeTest

/** The [SessionGatewayStoreContract] against a real Postgres (gated — see [PgTestGate]). */
class SessionGatewayStorePgTest : SessionGatewayStoreContract() {
    private lateinit var pool: ClaimJdbcPool
    override lateinit var store: SessionGatewayStore

    @BeforeTest
    fun setUp() {
        val url = PgTestGate.urlOrSkip()
        runBlocking {
            pool = ClaimJdbcPool(url)
            store = JdbcSessionGatewayStore(pool).also { it.prepare() }
            pool.withConnection { conn -> conn.createStatement().use { it.execute("TRUNCATE session_gateway") } }
        }
    }

    @AfterTest
    fun tearDown() {
        if (::pool.isInitialized) pool.close()
    }
}
