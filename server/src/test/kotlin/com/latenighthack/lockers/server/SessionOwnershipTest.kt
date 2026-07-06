package com.latenighthack.lockers.server

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isFalse
import assertk.assertions.isTrue
import com.latenighthack.ktbuf.net.GrpcRequestContext
import com.latenighthack.ktbuf.net.StreamControlEvent
import com.latenighthack.ktcrypto.Secp256r1KeyPair
import com.latenighthack.ktcrypto.encode
import com.latenighthack.ktcrypto.generate
import com.latenighthack.ktstore.InMemoryStoreDelegate
import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.push.v1.PushGatewayService
import com.latenighthack.lockers.server.cluster.RingSessionOwnership
import com.latenighthack.lockers.server.services.push.v1.PushGatewayDiscovery
import com.latenighthack.lockers.server.services.session.v1.SessionInboxStoreImpl
import com.latenighthack.lockers.server.services.session.v1.SessionOwner
import com.latenighthack.lockers.server.services.session.v1.SessionOwnership
import com.latenighthack.lockers.server.services.session.v1.SessionServiceImpl
import com.latenighthack.lockers.server.services.session.v1.SessionStoreImpl
import com.latenighthack.lockers.server.storage.v1.ServerSession
import com.latenighthack.lockers.server.storage.v1.ServerSessionId
import com.latenighthack.lockers.session.v1.WatchSessionRequest
import com.latenighthack.lockers.session.v1.WatchSessionResponse
import com.latenighthack.lockers.sharding.NodeId
import com.latenighthack.lockers.sharding.ShardCounts
import com.latenighthack.lockers.sharding.inmem.SimCluster
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.random.Random
import kotlin.test.Test

/**
 * M4b: a session open is gated to the node that owns the session's shard on the session ring. A
 * non-owner returns EPOCH_STALE + a redirect to the owner (so the client's reconnect loop
 * re-resolves); the owner proceeds through normal validation. Mirrors RoomOwnershipTest for the
 * session axis.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class SessionOwnershipTest {

    private val noPushGateway = object : PushGatewayDiscovery {
        override suspend fun findServer(sessionId: SessionId): PushGatewayService? = null
    }

    private suspend fun sessionImplWith(ownership: SessionOwnership): Pair<SessionServiceImpl, SessionStoreImpl> {
        val delegate = InMemoryStoreDelegate()
        val sessionStore = SessionStoreImpl(delegate).also { it.prepare() }
        val inboxStore = SessionInboxStoreImpl(delegate).also { it.prepare() }
        delegate.createStores()
        val impl = SessionServiceImpl(
            sessionStore, inboxStore, SimpleMeterRegistry(), noPushGateway, ownership, LockersConfig.defaults(),
        )
        return impl to sessionStore
    }

    /** Drives watchSession with a single Open request and returns the first Open response emitted. */
    private suspend fun firstOpen(
        impl: SessionServiceImpl,
        open: WatchSessionRequest,
    ): WatchSessionResponse.Open {
        val responses = impl.watchSession(
            GrpcRequestContext("", emptyMap(), emptyMap(), emptyMap(), com.latenighthack.lockers.session.v1.SessionServer.Descriptor, com.latenighthack.lockers.session.v1.SessionServer.Descriptor.methods[0]),
            flow {
                emit(open)
                // Keep the client stream open; the impl closes it itself on a rejected open.
                kotlinx.coroutines.awaitCancellation()
            },
        )
        val message = responses.first { it is StreamControlEvent.Message } as StreamControlEvent.Message
        return message.message.response?.getOpen()!!
    }

    private suspend fun seedSession(store: SessionStoreImpl, rawId: ByteArray, keyPair: Secp256r1KeyPair, material: ByteArray) {
        store.updateSession(ServerSession {
            sessionId = ServerSessionId(rawId)
            nextKeyMaterial = material
            authorizedPublicKey = keyPair.publicKey.encode()
        })
    }

    private suspend fun openRequest(rawId: ByteArray, keyPair: Secp256r1KeyPair, material: ByteArray): WatchSessionRequest {
        val encodedPublicKey = keyPair.publicKey.encode()
        val sig = keyPair.privateKey.sign(material)
        return WatchSessionRequest {
            request.open {
                sessionId { rawValue = rawId }
                sequenceKeySignature {
                    publicKey { rawValue = encodedPublicKey }
                    signature = sig
                }
            }
        }
    }

    @Test
    fun `session open on a non-owner node returns EPOCH_STALE and a redirect`() = runBlocking {
        val (impl, store) = sessionImplWith(object : SessionOwnership {
            override suspend fun resolve(sessionId: SessionId) =
                SessionOwner.Remote(address = "session-owner:8080", epoch = 4)
        })
        val rawId = Random.nextBytes(32)
        val kp = Secp256r1KeyPair.generate()
        val material = Random.nextBytes(32)
        seedSession(store, rawId, kp, material)

        val open = firstOpen(impl, openRequest(rawId, kp, material))

        assertThat(open.result is WatchSessionResponse.Open.Result.EPOCH_STALE).isTrue()
        assertThat(open.redirect?.ownerAddress).isEqualTo("session-owner:8080")
        assertThat(open.redirect?.epoch).isEqualTo(4L)
        impl.close()
    }

    @Test
    fun `session open on the owning node proceeds past the gate`() = runBlocking {
        val (impl, store) = sessionImplWith(object : SessionOwnership {
            override suspend fun resolve(sessionId: SessionId) = SessionOwner.Local
        })
        val rawId = Random.nextBytes(32)
        val kp = Secp256r1KeyPair.generate()
        val material = Random.nextBytes(32)
        seedSession(store, rawId, kp, material)

        val open = firstOpen(impl, openRequest(rawId, kp, material))

        // The gate did NOT fire on the owner: the open proceeds into normal validation and never
        // returns EPOCH_STALE nor a redirect (which only the non-owner path sets).
        assertThat(open.result is WatchSessionResponse.Open.Result.EPOCH_STALE).isFalse()
        assertThat(open.redirect).isEqualTo(null)
        impl.close()
    }

    @Test
    fun `RingSessionOwnership reports local vs remote exactly per the session ring`() = runTest {
        val sim = SimCluster(listOf(NodeId("a"), NodeId("b"), NodeId("c")), ShardCounts(64))
        val router = sim.routerFor(NodeId("a"), backgroundScope)
        runCurrent()
        val ownership = RingSessionOwnership(router)

        var localSeen = 0
        var remoteSeen = 0
        repeat(300) { i ->
            val sessionId = SessionId("s-$i".encodeToByteArray())
            val resolved = ownership.resolve(sessionId)
            val route = router.routeSession(sessionId.rawValue)
            if (route.isLocal) {
                assertThat(resolved).isEqualTo(SessionOwner.Local)
                localSeen++
            } else {
                assertThat(resolved is SessionOwner.Remote).isTrue()
                val remote = resolved as SessionOwner.Remote
                assertThat(remote.address).isEqualTo("${route.address!!.host}:${route.address!!.port}")
                remoteSeen++
            }
        }
        assertThat(localSeen > 0).isTrue()
        assertThat(remoteSeen > 0).isTrue()
    }
}
