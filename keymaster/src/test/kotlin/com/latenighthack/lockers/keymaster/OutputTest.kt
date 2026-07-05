package com.latenighthack.lockers.keymaster

import assertk.assertThat
import assertk.assertions.isEqualTo
import kotlin.test.Test

class OutputTest {
    @Test
    fun base64RoundTripsSoIdsCanBePastedBack() {
        val bytes = byteArrayOf(0, 1, 2, 3, 127, -1, -128)
        assertThat(decodeB64(bytes.toB64()).toList()).isEqualTo(bytes.toList())
    }

    @Test
    fun toB64ProducesStandardPaddedEncoding() {
        assertThat(byteArrayOf(1, 2, 3).toB64()).isEqualTo("AQID")
    }
}
