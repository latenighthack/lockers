package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.push.v1.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlin.test.*

class ReviewPushControllerTests {
    @Test fun `push registration retries a transient failure without changing session ID`() = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        val store = PushRegistrationStoreImpl(db); store.prepare()
        var calls = 0
        val rpc = ReviewRpc { _, _ ->
            calls++
            if (calls == 1) throw FaultInjectingRpcClient.rpcError(Codes.UNAVAILABLE)
            RegisterSessionResponse(result = RegisterSessionResponse.Result.OK).toByteArray()
        }
        val controller = PushRegistrationController(rpc, store, MutableStateFlow(SessionId(byteArrayOf(1))))
        try {
            controller.register(PushRegistrations.fcm("token"))
            controller.start()
            withTimeout(2_000) { controller.awaitRegistered(PushBackendType.FCM) }
            assertEquals(2, calls)
            assertFalse(store.getRegistration(2)!!.isPending)
        } finally { controller.closeAndJoin() }
    }
}
