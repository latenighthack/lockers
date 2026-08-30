package com.latenighthack.lockers.server.claim

/**
 * The store contracts against the in-memory doubles. This is what licenses using them as stand-ins
 * for Postgres in the two-node harness — any semantic drift from [JdbcRoomClaimStore] fails here
 * or in the PG contract run.
 */
class InMemoryRoomClaimStoreTest : RoomClaimStoreContract() {
    override val store: RoomClaimStore = InMemoryRoomClaimStore()
}

class InMemorySessionGatewayStoreTest : SessionGatewayStoreContract() {
    override val store: SessionGatewayStore = InMemorySessionGatewayStore()
}
