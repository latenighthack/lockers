package com.latenighthack.kitkit.server

import io.ktor.server.application.install
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSockets

fun main() {
    val serverCore = ServerCore::class.create()

    embeddedServer(CIO, port = 8080) {
        install(WebSockets)
        routing {
            monolith(serverCore)
        }
    }.start(wait = true)
}
