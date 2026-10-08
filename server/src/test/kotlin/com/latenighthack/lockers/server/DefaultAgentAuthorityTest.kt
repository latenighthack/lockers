package com.latenighthack.lockers.server

import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.agents.ExampleLockerAgent
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

class DefaultAgentAuthorityTest {
    @Test fun `default graph cannot derive privileged writes from untrusted lobby writes`() = runBlocking {
        val core = ServerCore::class.create(LockersConfig.defaults(), ServerStorage.inMemory())
        val id = LockerId { rawValue = byteArrayOf(1); keyspace = LockerKeyspace { value = 31 } }
        val locker = Locker { open { encodedPayload = byteArrayOf(7) } }
        assertEquals(emptyList(), core.agentRegistry.processPayload(RoomId(byteArrayOf(1)), id, locker))
        // Explicit embedders choose the trusted registry before service construction.
        val optedIn = ServerCore::class.create(LockersConfig.defaults(), ServerStorage.inMemory())
        optedIn.overrideAgentRegistry = ExampleLockerAgent()
        assertEquals(30L, optedIn.agentRegistry.processPayload(RoomId(byteArrayOf(1)), id, locker).single().lockerId.keyspace?.value)
    }
}
