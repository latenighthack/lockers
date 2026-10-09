package com.latenighthack.lockers.server

import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.broadcast.v1.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.push.v1.PushGatewayService
import com.latenighthack.lockers.server.services.push.v1.PushGatewayDiscovery
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.runBlocking
import kotlin.test.*

class BroadcastShapeTest {
    @Test fun `durable bodyless broadcast preserves its full metadata without inventing a locker scope`(): Unit = runBlocking {
        val db = ServerStorage.inMemory()
        val sessions = SessionStoreImpl(db).also { it.prepare() }; val inbox = SessionInboxStoreImpl(db).also { it.prepare() }
        db.open()
        val sid = SessionId(byteArrayOf(7)); val pair = Secp256r1KeyPair.generate()
        sessions.updateSession(ServerSession(ServerSessionId(sid.rawValue), ByteArray(32), pair.publicKey.encode()))
        val service = SessionServiceImpl(sessions, inbox, SimpleMeterRegistry(), object: PushGatewayDiscovery {
            override suspend fun findServer(sessionId: SessionId): PushGatewayService? = null
        }, LocalSessionOwnership(), LockersConfig.defaults().copy(deliveryWorkerEnabled = false))
        try {
            service.start()
            val notification = Notification { payload { rawValue = byteArrayOf(1, 2, 3) }; push { body = "Only body" } }
            assertEquals(1L, LocalBroadcastAdminServiceRpc(service).broadcast(BroadcastRequest {
                this.notification = notification; target { kind.sessions { sessionIds = listOf(sid) } }
            }).delivered)
            val original = inbox.getAllClientEvents(ServerSessionId(sid.rawValue)).single()
            assertNull(original.locker); assertTrue(original.roomId!!.rawValue.isEmpty())
            assertEquals(notification, original.notification)
            assertTrue(original.eventId!!.rawValue.size in 16..64)
            val reopened = SessionInboxStoreImpl(db)
            assertContentEquals(original.toByteArray(), reopened.getAllClientEvents(ServerSessionId(sid.rawValue)).single().toByteArray())
        } finally { service.closeAndJoin(); db.close() }
    }
}
