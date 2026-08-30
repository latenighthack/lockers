package com.latenighthack.lockers.server.claim

import kotlinx.coroutines.runBlocking
import kotlin.test.AfterTest
import kotlin.test.BeforeTest

/**
 * The [RoomClaimStoreContract] against a real Postgres (gated — see [PgTestGate]). The claim SQL's
 * ON CONFLICT / conditional-WHERE / RETURNING semantics only exist on Postgres, so this run is the
 * one that validates the production store.
 */
class RoomClaimStorePgTest : RoomClaimStoreContract() {
    private lateinit var pool: ClaimJdbcPool
    override lateinit var store: RoomClaimStore

    @BeforeTest
    fun setUp() {
        val url = PgTestGate.urlOrSkip()
        runBlocking {
            pool = ClaimJdbcPool(url)
            store = JdbcRoomClaimStore(pool).also { it.prepare() }
            pool.withConnection { conn -> conn.createStatement().use { it.execute("TRUNCATE room_claim") } }
        }
    }

    @AfterTest
    fun tearDown() {
        if (::pool.isInitialized) pool.close()
    }
}
