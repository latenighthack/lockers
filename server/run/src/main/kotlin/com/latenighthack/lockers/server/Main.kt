package com.latenighthack.lockers.server

import com.latenighthack.ktstore.InMemoryStoreDelegate
import io.ktor.server.application.install
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSockets
import kotlinx.coroutines.runBlocking

fun main() {
    runBlocking {
        val core = ServerCore::class.create(InMemoryStoreDelegate())
        core.setup()

        val component = MonolithComponent(core)
        component.start()

        embeddedServer(CIO, port = 8080) {
            install(WebSockets)
            routing {
                monolith(component)
            }
        }.start(wait = true)
    }
}
