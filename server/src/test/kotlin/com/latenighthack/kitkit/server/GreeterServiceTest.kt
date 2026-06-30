package com.latenighthack.kitkit.server

import com.latenighthack.kitkit.greeter.v1.GreetRequest
import com.latenighthack.kitkit.greeter.v1.GreeterServiceRpc
import com.latenighthack.ktbuf.rpc.HttpRpcClient
import com.latenighthack.ktbuf.test.server.runTestWithServer
import io.ktor.server.application.Application
import io.ktor.server.routing.routing
import kotlin.test.Test
import kotlin.test.assertEquals

/** Boots the real monolith router over an embedded Ktor server, exactly as production does. */
private suspend fun Application.attachTestServices() {
    val serverCore = ServerCore::class.create()
    routing {
        monolith(serverCore)
    }
}

class GreeterServiceTest {
    @Test
    fun greetsOverTheWireAndCountsViaInjectedStore() = runTestWithServer(Application::attachTestServices) { server, _ ->
        val greeter = GreeterServiceRpc(HttpRpcClient(server.serverUrl))

        // First call: the injected GreetingStore is at count 0 -> returns 1.
        val first = greeter.greet(GreetRequest { name = "Ada" })
        assertEquals("Hello, Ada! You are greeting #1", first.message)

        // Second call hits the SAME singleton store from the graph -> returns 2.
        val second = greeter.greet(GreetRequest { name = "Grace" })
        assertEquals("Hello, Grace! You are greeting #2", second.message)
    }
}
