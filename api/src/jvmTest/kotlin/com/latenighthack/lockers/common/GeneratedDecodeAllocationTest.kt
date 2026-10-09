package com.latenighthack.lockers.common

import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.review.fixture.*
import java.lang.management.ManagementFactory
import kotlin.test.*

/** Allocation, rather than elapsed time, makes the quadratic-copy regression deterministic. */
class GeneratedDecodeAllocationTest {
    @Test fun repeatedPackedValuesAccumulateWithoutQuadraticCopies() {
        val count = 10_000
        val bytes = byteArrayOf(10, 0x90.toByte(), 0x4e) + ByteArray(count) { 1 }
        PackedEnumFixture.fromByteArray(byteArrayOf(10, 1, 1)) // warm initialization
        val (decoded, allocated) = allocated { PackedEnumFixture.fromByteArray(bytes) }
        assertEquals(count, decoded.selections.size)
        assertTrue(decoded.selections.all { it == PackedEnumFixture.Selection.SECOND })
        assertTrue(allocated < 16L * 1024 * 1024, "10k values allocated $allocated bytes")
    }
    @Test fun unknownFieldsAccumulateWithoutQuadraticCopies() {
        val bytes = ByteArray(30_000) { when (it % 3) { 0 -> 0x98.toByte(); 1 -> 6; else -> 1 } }
        RoomId.fromByteArray(byteArrayOf(0x98.toByte(), 6, 1))
        val (decoded, allocated) = allocated { RoomId.fromByteArray(bytes) }
        assertContentEquals(bytes, decoded.unknownFields)
        assertContentEquals(bytes, decoded.toByteArray())
        assertTrue(allocated < 16L * 1024 * 1024, "30KiB unknown fields allocated $allocated bytes")
    }
    private fun <T> allocated(block: () -> T): Pair<T, Long> {
        val bean = ManagementFactory.getThreadMXBean() as com.sun.management.ThreadMXBean
        assertTrue(bean.isThreadAllocatedMemorySupported)
        bean.isThreadAllocatedMemoryEnabled = true
        val id = Thread.currentThread().id
        val before = bean.getThreadAllocatedBytes(id)
        val result = block()
        return result to (bean.getThreadAllocatedBytes(id) - before)
    }
}
