package com.latenighthack.lockers.server.claim

import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.server.storage.v1.ServerSessionId
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.*
import kotlin.test.*

@OptIn(ExperimentalCoroutinesApi::class)
class RegistryIncarnationTest {
    @Test fun `queued old detach cannot delete a replacement on the same node`() = runTest {
        val store = InMemorySessionGatewayStore()
        val registry = ClaimSessionRegistry(store, "node", "node:8081", 60000, backgroundScope)
        val id = ServerSessionId(byteArrayOf(8))
        registry.attachBeforeSnapshot(id)
        registry.detach(id) // Queued cleanup has not executed yet.
        registry.attachBeforeSnapshot(id)
        runCurrent()
        assertNotNull(store.lookup(SessionId(id.rawValue)))
    }
}
