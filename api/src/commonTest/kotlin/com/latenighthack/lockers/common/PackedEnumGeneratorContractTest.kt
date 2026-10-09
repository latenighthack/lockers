package com.latenighthack.lockers.common

import com.latenighthack.lockers.review.fixture.*
import kotlin.test.*

class PackedEnumGeneratorContractTest {
    @Test fun packedEnumReaderConsumesOnlyItsField() {
        val expected = PackedEnumFixture(selections = listOf(PackedEnumFixture.Selection.SECOND, PackedEnumFixture.Selection.THIRD), marker = 99)
        assertEquals(expected, PackedEnumFixture.fromByteArray(expected.toByteArray()))
        val unknown = PackedEnumFixture(selections = listOf(PackedEnumFixture.Selection.fromInt(777)), marker = 12)
        assertEquals(unknown, PackedEnumFixture.fromByteArray(unknown.toByteArray()))
    }
}
