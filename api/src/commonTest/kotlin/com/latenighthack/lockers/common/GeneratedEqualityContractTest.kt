package com.latenighthack.lockers.common

import com.latenighthack.lockers.common.v1.*
import kotlin.test.*

class GeneratedEqualityContractTest {
    @Test fun unknownFieldsHaveSymmetricEqualityAndConsistentHashing() {
        val plain = RoomId(byteArrayOf(1))
        val future = plain.copy(unknownFields = byteArrayOf(0x98.toByte(), 6, 1))
        assertFalse(plain.equals(future))
        assertFalse(future.equals(plain))
        val empty = plain.copy(unknownFields = byteArrayOf())
        assertEquals(plain, empty)
        assertEquals(empty, plain)
        assertEquals(plain.hashCode(), empty.hashCode())
        assertEquals(future.hashCode(), future.copy().hashCode())
    }
    @Test fun enumEqualityIsBoundToItsDeclaredFamily() {
        val scope: LockScopeKind = LockScopeKind.LOCK_SCOPE_ROOM
        val other: Any = com.latenighthack.lockers.room.v1.PostLockerChangeResponse.Result.SIGNATURE_REQUIRED
        assertFalse(scope.equals(2))
        assertFalse(scope.equals(other))
        assertEquals(LockScopeKind.LOCK_SCOPE_ROOM, LockScopeKind.fromInt(2))
    }
}
