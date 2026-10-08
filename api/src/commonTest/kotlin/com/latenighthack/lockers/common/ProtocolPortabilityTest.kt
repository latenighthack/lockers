package com.latenighthack.lockers.common

import com.latenighthack.lockers.common.v1.*
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

class ProtocolPortabilityTest {
    @Test fun lockerIdentityRetainsBinaryKeyspaceAcrossProtoSerialization() {
        val id = LockerId(byteArrayOf(0, -1, 127), LockerKeyspace(Long.MAX_VALUE))
        val decoded = LockerId.fromByteArray(id.toByteArray())
        assertContentEquals(id.rawValue, decoded.rawValue)
        assertEquals(Long.MAX_VALUE, decoded.keyspace?.value)
    }
}
