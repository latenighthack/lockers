package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktbuf.rpc.HttpRpcClient
import com.latenighthack.ktbuf.test.server.TestServer
import com.latenighthack.ktstore.InMemoryStoreDelegate
import io.ktor.server.application.Application
import io.ktor.server.routing.routing

/**
 * Boots the real locker monolith over an embedded Ktor server backed by
 * in-memory stores — the same router production uses. Consumed by the connector
 * integration tests via `runTestWithServer(Application::attachTestServices)`.
 */
suspend fun Application.attachTestServices() {
    val core = ServerCore::class.create(InMemoryStoreDelegate())

    core.setup()

    routing {
        monolith(core)
    }
}

val TestServer.rpcClient: RpcClient get() = HttpRpcClient(serverUrl)
