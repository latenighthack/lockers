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
    @Test fun `late register response cannot restore an unregistered credential`() = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        val store = PushRegistrationStoreImpl(db); store.prepare()
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val registrationReturned = CompletableDeferred<Unit>()
        val unregistered = CompletableDeferred<Unit>()
        val rpc = ReviewRpc { method, _ -> when (method.methodName) {
            "RegisterSession" -> {
                entered.complete(Unit)
                withContext(NonCancellable) { release.await() }
                registrationReturned.complete(Unit)
                RegisterSessionResponse(result = RegisterSessionResponse.Result.OK).toByteArray()
            }
            "UnregisterSession" -> { unregistered.complete(Unit); UnregisterSessionResponse(result = UnregisterSessionResponse.Result.OK).toByteArray() }
            else -> error(method.methodName)
        } }
        val controller = PushRegistrationController(rpc, store, MutableStateFlow(SessionId(byteArrayOf(1))))
        try {
            controller.register(PushRegistrations.fcm("obsolete")); controller.start(); entered.await()
            controller.unregister(PushBackendType.FCM)
            release.complete(Unit); registrationReturned.await(); unregistered.await(); delay(50)
            assertNull(store.getRegistration(2), "An obsolete successful reply must not resurrect local desired state")
        } finally { release.complete(Unit); controller.closeAndJoin() }
    }

    @Test fun `offline removal persists its revision and retries after controller replacement`() = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        val store = PushRegistrationStoreImpl(db); store.prepare()
        val offline = PushRegistrationController(ReviewRpc { _, _ -> error("offline") }, store, MutableStateFlow(null))
        try { offline.register(PushRegistrations.fcm("token")); offline.unregister(PushBackendType.FCM) }
        finally { offline.closeAndJoin() }
        val intent = store.getAllIntents().single()
        assertEquals(2, intent.revision)
        assertTrue(intent.encodedRegistration.isEmpty() && intent.pending)
        var calls = 0
        val resumed = PushRegistrationController(ReviewRpc { method, bytes ->
            assertEquals("UnregisterSession", method.methodName)
            assertEquals(2, UnregisterSessionRequest.fromByteArray(bytes).credentialRevision)
            if (++calls == 1) throw FaultInjectingRpcClient.rpcError(Codes.UNAVAILABLE)
            UnregisterSessionResponse(result = UnregisterSessionResponse.Result.OK).toByteArray()
        }, store, MutableStateFlow(SessionId(byteArrayOf(1))))
        try {
            resumed.start(); withTimeout(2_000) { resumed.awaitUnregistered(PushBackendType.FCM) }
            assertEquals(2, calls)
            assertFalse(store.getAllIntents().single().pending)
            assertNull(store.getRegistration(2))
        } finally { resumed.closeAndJoin() }
    }

    @Test fun `a rotated credential remains desired after an obsolete reply`() = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open()
        val store = PushRegistrationStoreImpl(db); store.prepare()
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val revisions = mutableListOf<Long>()
        val rpc = ReviewRpc { _, bytes ->
            val request = RegisterSessionRequest.fromByteArray(bytes)
            revisions += request.credentialRevision
            if (request.credentialRevision == 1L) { entered.complete(Unit); withContext(NonCancellable) { release.await() } }
            RegisterSessionResponse(result = RegisterSessionResponse.Result.OK).toByteArray()
        }
        val controller = PushRegistrationController(rpc, store, MutableStateFlow(SessionId(byteArrayOf(1))))
        try {
            controller.register(PushRegistrations.fcm("old")); entered.await()
            controller.register(PushRegistrations.fcm("new")); release.complete(Unit)
            withTimeout(2_000) { controller.awaitRegistered(PushBackendType.FCM) }
            assertEquals(listOf(1L, 2L), revisions)
            assertContentEquals(PushRegistrations.fcm("new").toByteArray(), store.getRegistration(2)!!.encodedRegistration)
            assertEquals(2, controller.registrations.value[PushBackendType.FCM]!!.revision)
        } finally { release.complete(Unit); controller.closeAndJoin() }
    }

}
