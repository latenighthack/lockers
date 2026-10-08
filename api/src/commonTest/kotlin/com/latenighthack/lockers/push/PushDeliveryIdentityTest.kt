package com.latenighthack.lockers.push

import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.push.v1.*
import kotlin.test.*

class PushDeliveryIdentityTest {
    @Test fun stableDeliveryIdentityUsesAdditiveFieldThree() {
        val request = SendPushRequest(sessionId = SessionId(byteArrayOf(1)), push = Push(title = "a"), deliveryId = byteArrayOf(2))
        val encoded = request.toByteArray()
        assertContentEquals(byteArrayOf(10,3,10,1,1,18,3,10,1,97,26,1,2), encoded)
        assertContentEquals(byteArrayOf(2), SendPushRequest.fromByteArray(encoded).deliveryId)
        val legacy = SendPushRequest.fromByteArray(encoded.copyOf(encoded.size - 3))
        assertContentEquals(byteArrayOf(), legacy.deliveryId)
        assertEquals("a", legacy.push?.title)
    }
}
