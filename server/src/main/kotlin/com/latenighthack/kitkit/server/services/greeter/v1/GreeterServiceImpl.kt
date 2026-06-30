package com.latenighthack.kitkit.server.services.greeter.v1

import com.latenighthack.kitkit.greeter.v1.GreetRequest
import com.latenighthack.kitkit.greeter.v1.GreetResponse
import com.latenighthack.kitkit.greeter.v1.GreeterServer
import com.latenighthack.kitkit.server.ServerCore
import com.latenighthack.kitkit.server.tools.GrpcRouteProvider
import com.latenighthack.kitkit.server.tools.ServiceScope
import com.latenighthack.ktbuf.net.GrpcRequestContext
import com.latenighthack.ktbuf.net.ServerDescriptor
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import me.tatarka.inject.annotations.Component
import me.tatarka.inject.annotations.Inject

/** A trivial dependency, provided by [ServerCore] and consumed by the service. */
interface GreetingStore {
    suspend fun recordGreeting(name: String): Long
}

class InMemoryGreetingStore : GreetingStore {
    private val mutex = Mutex()
    private var count = 0L

    override suspend fun recordGreeting(name: String): Long = mutex.withLock { ++count }
}

/**
 * The pluggable service module: a kotlin-inject `@Component` whose parent is the
 * [ServerCore] graph. kotlin-inject constructs [GreeterServiceImpl], satisfying
 * its constructor from the parent graph, and exposes it as a [GrpcRouteProvider]
 * the monolith can mount.
 */
@ServiceScope
@Component
abstract class GreeterServiceModule(
    @Component val serverCore: ServerCore
) : GrpcRouteProvider<GreeterServer> {
    abstract val serverImpl: GreeterServiceImpl

    override val server: GreeterServer get() = serverImpl
    override val descriptor: ServerDescriptor = GreeterServer.Descriptor
}

/**
 * The actual RPC handler. It consumes [GreetingStore] purely through the
 * injection graph — it never constructs one, and doesn't know whether the
 * store is in-memory, postgres-backed, or a remote service.
 */
@ServiceScope
@Inject
class GreeterServiceImpl(
    private val greetingStore: GreetingStore
) : GreeterServer {
    override suspend fun greet(context: GrpcRequestContext, request: GreetRequest): GreetResponse {
        val count = greetingStore.recordGreeting(request.name)
        return GreetResponse {
            message = "Hello, ${request.name}! You are greeting #$count"
        }
    }
}
