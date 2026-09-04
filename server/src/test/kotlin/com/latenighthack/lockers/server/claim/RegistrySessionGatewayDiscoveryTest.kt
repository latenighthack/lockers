package com.latenighthack.lockers.server.claim

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isInstanceOf
import assertk.assertions.isNotNull
import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.server.cluster.PeerConnectionPool
import com.latenighthack.lockers.session.v1.LocalSessionGatewayServiceRpc
import com.latenighthack.lockers.session.v1.PostEventRequest
import com.latenighthack.lockers.session.v1.PostEventResponse
import com.latenighthack.lockers.session.v1.SessionGatewayServer
import com.latenighthack.ktbuf.net.GrpcRequestContext
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertFailsWith

class RegistrySessionGatewayDiscoveryTest {
    private val registry = SimpleMeterRegistry()
    private val meters = ClaimMetrics(registry)

    private val localServer = object : SessionGatewayServer {
        override suspend fun postEvent(context: GrpcRequestContext, request: PostEventRequest): PostEventResponse =
            PostEventResponse { }
    }

    private class CountingSessionStore(private val delegate: SessionGatewayStore) : SessionGatewayStore by delegate {
        val lookups = AtomicInteger(0)
        override suspend fun lookup(sessionId: SessionId): SessionGatewayRow? {
            lookups.incrementAndGet()
            return delegate.lookup(sessionId)
        }
    }

    private fun session(name: String) = SessionId(name.encodeToByteArray())

    private fun discovery(store: SessionGatewayStore, cacheTtlMs: Long = 5_000) =
        RegistrySessionGatewayDiscovery(localServer, store, PeerConnectionPool(), "self", meters, cacheTtlMs)

    @Test
    fun `a locally-homed session short-circuits to the in-process gateway`(): Unit = runBlocking {
        val store = InMemorySessionGatewayStore()
        store.upsert(session("s1"), "self", "self:1", ttlMs = 60_000)
        // NB: `: Unit` matters — runBlocking returns the last expression, and a @Test method
        // with a non-void return type is silently skipped by the JUnit platform.
        assertThat(discovery(store).findServer(session("s1")))
            .isNotNull().isInstanceOf(LocalSessionGatewayServiceRpc::class)
    }

    @Test
    fun `a remotely-homed session resolves a peer stub`() = runBlocking {
        val store = InMemorySessionGatewayStore()
        store.upsert(session("s1"), "peer", "peer-host:9999", ttlMs = 60_000)
        val server = discovery(store).findServer(session("s1"))
        assertThat(server).isNotNull()
        assertThat(server is LocalSessionGatewayServiceRpc).isEqualTo(false)
    }

    @Test
    fun `a missing or expired row means offline - falls back to the local gateway`(): Unit = runBlocking {
        val store = InMemorySessionGatewayStore()
        // Offline delivery goes through the in-process gateway (durable inbox from any node);
        // the miss is still metered so offline lookup volume stays observable.
        assertThat(discovery(store).findServer(session("absent")))
            .isNotNull().isInstanceOf(LocalSessionGatewayServiceRpc::class)
        assertThat(meters.registryMisses.count()).isEqualTo(1.0)

        store.upsert(session("expired"), "peer", "peer-host:9999", ttlMs = 50)
        delay(150)
        assertThat(discovery(store).findServer(session("expired")))
            .isNotNull().isInstanceOf(LocalSessionGatewayServiceRpc::class)
    }

    @Test
    fun `rows are cached for the configured TTL and re-fetched after`() = runBlocking {
        val store = CountingSessionStore(InMemorySessionGatewayStore())
        store.upsert(session("s1"), "peer", "peer-host:9999", ttlMs = 60_000)
        val discovery = discovery(store, cacheTtlMs = 100)
        discovery.findServer(session("s1"))
        discovery.findServer(session("s1"))
        assertThat(store.lookups.get()).isEqualTo(1)
        delay(250)
        discovery.findServer(session("s1"))
        assertThat(store.lookups.get()).isEqualTo(2)
    }

    @Test
    fun `malformed node_addr is rejected loudly`() {
        assertFailsWith<IllegalArgumentException> { parsePeerAddress("no-port") }
        assertFailsWith<IllegalArgumentException> { parsePeerAddress(":123") }
        assertThat(parsePeerAddress("host:123").host).isEqualTo("host")
        assertThat(parsePeerAddress("host:123").port).isEqualTo(123)
    }
}
