package com.latenighthack.lockers.server

import assertk.assertThat
import assertk.assertions.containsExactlyInAnyOrder
import assertk.assertions.hasSize
import assertk.assertions.isEmpty
import assertk.assertions.isEqualTo
import com.latenighthack.ktstore.InMemoryStoreDelegate
import com.latenighthack.lockers.broadcast.v1.BroadcastRequest
import com.latenighthack.lockers.broadcast.v1.LocalBroadcastAdminServiceRpc
import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.push.v1.PushGatewayService
import com.latenighthack.lockers.push.v1.SendPushRequest
import com.latenighthack.lockers.push.v1.SendPushResponse
import com.latenighthack.lockers.server.services.push.v1.PushGatewayDiscovery
import com.latenighthack.lockers.server.services.session.v1.SessionInboxStoreImpl
import com.latenighthack.lockers.server.services.session.v1.SessionServiceImpl
import com.latenighthack.lockers.server.services.session.v1.SessionStoreImpl
import com.latenighthack.lockers.server.storage.v1.ServerSession
import com.latenighthack.lockers.server.storage.v1.ServerSessionId
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertFailsWith

class BroadcastServiceTest {

    // Records the sessions a push was sent to; every session is reachable.
    private class RecordingPushGateway : PushGatewayDiscovery, PushGatewayService {
        val sent = mutableListOf<ByteArray>()
        override suspend fun findServer(sessionId: SessionId): PushGatewayService = this
        override suspend fun sendPush(request: SendPushRequest): SendPushResponse {
            request.sessionId?.rawValue?.let { sent += it }
            return SendPushResponse { result = SendPushResponse.Result.OK }
        }
    }

    private class Harness(
        val impl: SessionServiceImpl,
        val sessionStore: SessionStoreImpl,
        val inboxStore: SessionInboxStoreImpl,
        val push: RecordingPushGateway,
        val admin: LocalBroadcastAdminServiceRpc,
    )

    private suspend fun harness(config: LockersConfig = LockersConfig.defaults()): Harness {
        val delegate = InMemoryStoreDelegate()
        val sessionStore = SessionStoreImpl(delegate)
        val inboxStore = SessionInboxStoreImpl(delegate)
        sessionStore.prepare()
        inboxStore.prepare()
        delegate.createStores()
        val push = RecordingPushGateway()
        val impl = SessionServiceImpl(sessionStore, inboxStore, SimpleMeterRegistry(), push, config)
        return Harness(impl, sessionStore, inboxStore, push, LocalBroadcastAdminServiceRpc(impl))
    }

    private suspend fun SessionStoreImpl.seed(vararg rawSessionIds: ByteArray) {
        for (raw in rawSessionIds) {
            updateSession(ServerSession {
                sessionId = ServerSessionId(raw)
                nextKeyMaterial = ByteArray(32)
                authorizedPublicKey = ByteArray(0)
            })
        }
    }

    @Test
    fun broadcastAll_enqueuesEmptyRoomEventAndPushToEverySession() = runBlocking {
        val h = harness()
        val a = byteArrayOf(1)
        val b = byteArrayOf(2)
        h.sessionStore.seed(a, b)

        val payload = byteArrayOf(9, 8, 7)
        val response = h.admin.broadcast(BroadcastRequest {
            notification {
                this.payload { rawValue = payload }
                push {
                    title = "Update available"
                    body = "A new version is ready"
                }
            }
            target { kind.all { } }
        })

        assertThat(response.delivered).isEqualTo(2L)

        for (raw in listOf(a, b)) {
            val events = h.inboxStore.getAllEvents(ServerSessionId(raw))
            assertThat(events).hasSize(1)
            assertThat(events[0].roomId?.rawValue?.toList() ?: emptyList()).isEmpty()
            assertThat(events[0].encodedPayload.toList()).isEqualTo(payload.toList())
        }

        assertThat(h.push.sent.map { it.toList() })
            .containsExactlyInAnyOrder(a.toList(), b.toList())

        h.impl.close()
    }

    @Test
    fun broadcastExplicitList_targetsOnlyListedSessions_noPushWhenAbsent() = runBlocking {
        val h = harness()
        val a = byteArrayOf(1)
        val b = byteArrayOf(2)
        h.sessionStore.seed(a, b)

        val payload = byteArrayOf(5)
        val response = h.admin.broadcast(BroadcastRequest {
            notification {
                this.payload { rawValue = payload }
            }
            target {
                kind.sessions { sessionIds = listOf(SessionId { rawValue = a }) }
            }
        })

        assertThat(response.delivered).isEqualTo(1L)
        assertThat(h.inboxStore.getAllEvents(ServerSessionId(a))).hasSize(1)
        assertThat(h.inboxStore.getAllEvents(ServerSessionId(b))).isEmpty()
        assertThat(h.push.sent).isEmpty()

        h.impl.close()
    }

    @Test
    fun broadcast_rejectsWhenAdminTokenConfiguredButMissing() = runBlocking {
        val h = harness(LockersConfig.defaults().copy(adminToken = "secret"))
        h.sessionStore.seed(byteArrayOf(1))

        assertFailsWith<SecurityException> {
            h.admin.broadcast(BroadcastRequest {
                notification { this.payload { rawValue = byteArrayOf(0) } }
                target { kind.all { } }
            })
        }

        h.impl.close()
    }
}
