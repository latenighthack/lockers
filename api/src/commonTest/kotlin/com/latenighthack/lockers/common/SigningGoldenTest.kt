package com.latenighthack.lockers.common

import com.latenighthack.lockers.common.v1.*
import kotlin.test.Test
import kotlin.test.assertEquals

/** Cross-language vectors use eight-byte lengths; generated protobuf bytes are not signed. */
class SigningGoldenTest {
    private fun hex(value: ByteArray) = value.joinToString("") { (it.toInt() and 255).toString(16).padStart(2, '0') }
    @Test fun legacyCanonicalPreimagesRemainWireCompatible() {
        val room = RoomId(byteArrayOf(1))
        val locker = LockerId(byteArrayOf(2, 3), LockerKeyspace(5))
        val scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM)
        assertEquals("00000000000000106c6f636b6572732f76312f77726974650000000000000001010000000000000002020300000000000000050000000000000007000000000000000104", hex(LockerSigning.writeContext(room, locker, 7, byteArrayOf(4))))
        assertEquals("00000000000000126c6f636b6572732f76312f726174636865740000000000000001010000000000000002020300000000000000050000000000000007000000000000000105", hex(LockerSigning.ratchetContext(room, locker, 7, byteArrayOf(5))))
        assertEquals("00000000000000106c6f636b6572732f76312f6772616e74000000000000000101000000000000000200000000000000000000000000000000000000000000000105", hex(LockerSigning.grantContext(room, scope, byteArrayOf(5))))
        assertEquals("00000000000000116c6f636b6572732f76312f756e6c6f636b000000000000000101000000000000000200000000000000000000000000000000", hex(LockerSigning.unlockContext(room, scope)))
    }
    @Test fun omittedKeyspaceHasTheSameCanonicalSigningIdentityAsZero() {
        val room = RoomId(byteArrayOf(1))
        val absent = LockerId(byteArrayOf(2, 3), null)
        val zero = LockerId(byteArrayOf(2, 3), LockerKeyspace(0))
        assertEquals(hex(LockerSigning.writeContext(room, absent, 7, byteArrayOf(4))),
            hex(LockerSigning.writeContext(room, zero, 7, byteArrayOf(4))))
    }
}
