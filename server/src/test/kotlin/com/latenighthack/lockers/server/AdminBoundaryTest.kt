package com.latenighthack.lockers.server

import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.server.routing.routing
import kotlinx.coroutines.runBlocking
import java.net.URI
import java.net.http.*
import kotlin.test.*

class AdminBoundaryTest {
    @Test fun absentAdminCredentialDoesNotExposeManagementRoutes(): Unit = runBlocking {
        fixture(null) { url -> assertEquals(404, post(url, "").statusCode()) }
    }
    @Test fun wrongAdminCredentialIsRejectedBeforeDispatch(): Unit = runBlocking {
        fixture("secret") { url ->
            assertEquals(401, post(url, "").statusCode())
            assertEquals(401, post(url, "wrong").statusCode())
            assertEquals(200, post(url, "secret").statusCode())
        }
    }
    private fun post(url: String, token: String): HttpResponse<String> {
        val request = HttpRequest.newBuilder(URI(url)).header("x-admin-token", token)
            .POST(HttpRequest.BodyPublishers.ofByteArray(byteArrayOf())).build()
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString())
    }
    private suspend fun fixture(token: String?, block: (String) -> Unit) {
        val db = ServerStorage.inMemory()
        val core = ServerCore::class.create(LockersConfig.defaults().copy(adminToken = token), db)
        core.overridePushProviders = emptyList()
        core.setup()
        val component = MonolithComponent(core)
        val descriptor = component.adminServices.first { it.descriptor.serviceName == "PushAdmin" }.descriptor
        val method = descriptor.methods.first { it.methodName == "GetQueueStats" }
        val server = embeddedServer(CIO, host = "127.0.0.1", port = 0) { routing { monolithAdmin(component) } }
        try {
            server.start(wait = false)
            val port = server.engine.resolvedConnectors().single().port
            block("http://127.0.0.1:$port/api/${descriptor.packageName}.${descriptor.serviceName}/${method.methodName}")
        } finally { server.stop(0, 1000); component.closeAndJoin(); db.close() }
    }
}
