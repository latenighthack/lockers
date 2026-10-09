package com.latenighthack.lockers.server

import io.ktor.server.application.install
import io.ktor.server.websocket.WebSockets
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.server.routing.routing
import kotlinx.coroutines.runBlocking
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import kotlin.test.assertEquals
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class GatewayBoundaryTest {
    @Test fun `peer credentials fail closed and compare exact values`() {
        assertFalse(validPeerToken("", ""))
        assertFalse(validPeerToken("secret", ""))
        assertFalse(validPeerToken("secret", "wrong"))
        assertTrue(validPeerToken("secret", "secret"))
    }

    @Test fun `internal peer HTTP routes require a credential before dispatch`() = runBlocking {
        val core = ServerCore::class.create(LockersConfig.defaults(), ServerStorage.inMemory())
        core.setup()
        val component = MonolithComponent(core)
        val server = embeddedServer(CIO, host = "127.0.0.1", port = 0) { routing { monolithPeer(component, "secret") } }
        server.start(wait = false)
        try {
            val port = server.engine.resolvedConnectors().single().port
            val descriptor = component.sessionGatewayServiceModule.descriptor
            val uri = URI("http://127.0.0.1:$port/api/${descriptor.packageName}.${descriptor.serviceName}/${descriptor.methods.last().methodName}")
            val client = HttpClient.newHttpClient()
            for (token in listOf("", "wrong")) {
                val request = HttpRequest.newBuilder(uri).header(PEER_TOKEN_HEADER, token).POST(HttpRequest.BodyPublishers.ofByteArray(byteArrayOf(10, 0))).build()
                val response = client.send(request, HttpResponse.BodyHandlers.ofString())
                assertEquals(401, response.statusCode(), "$uri ${response.body()} ${response.headers()}")
            }
        } finally { server.stop(0, 1000); component.stop() }
    }

    @Test fun `default monolith router does not expose privileged gateway RPCs`(): Unit = runBlocking {
        val core = ServerCore::class.create(LockersConfig.defaults(), ServerStorage.inMemory())
        core.setup()
        val component = MonolithComponent(core)
        val server = embeddedServer(CIO, host = "127.0.0.1", port = 0) { install(WebSockets); routing { monolith(component) } }
        server.start(wait = false)
        try {
            val port = server.engine.resolvedConnectors().single().port
            val descriptor = component.sessionGatewayServiceModule.descriptor
            val uri = URI("http://127.0.0.1:$port/api/${descriptor.packageName}.${descriptor.serviceName}/${descriptor.methods.last().methodName}")
            val request = HttpRequest.newBuilder(uri).POST(HttpRequest.BodyPublishers.ofByteArray(byteArrayOf(10, 0))).build()
            val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString())
            assertEquals(404, response.statusCode(), "Public convenience routing must preserve the peer authentication boundary")
        } finally { server.stop(0, 1000); component.stop() }
    }

    @Test fun `delivery gateways are absent from public service routes`() {
        val component = MonolithComponent(ServerCore::class.create(LockersConfig.defaults(), ServerStorage.inMemory()))
        try {
            val public = component.clientServices.map { it.descriptor.serviceName }
            assertFalse("SessionGateway" in public, "SessionGateway accepts privileged inbox writes")
            assertFalse("PushGateway" in public, "PushGateway accepts privileged sends")
            assertTrue(component.allServices.any { it.descriptor.serviceName == "SessionGateway" })
            assertTrue(component.allServices.any { it.descriptor.serviceName == "PushGateway" })
        } finally { component.stop() }
    }
}
