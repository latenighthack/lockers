package com.latenighthack.lockers.connector.test

import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktbuf.net.RpcMethodSpecifier
import com.latenighthack.ktbuf.net.RpcResponse
import com.latenighthack.ktbuf.net.RpcResponseException
import com.latenighthack.ktbuf.net.RpcServerStream
import com.latenighthack.ktbuf.proto.Codes
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * A test [RpcClient] decorator that can inject faults on unary calls. Before each
 * unary call [onUnary] is invoked with the method and the 1-based attempt count for
 * that method name; it may throw (e.g. via [rpcError]) to simulate a server/network
 * fault, or return normally to pass the call through to [delegate]. Streaming calls
 * (the session watch) always pass through.
 */
class FaultInjectingRpcClient(
    private val delegate: RpcClient,
    private val onUnary: (method: RpcMethodSpecifier, attempt: Int) -> Unit = { _, _ -> }
) : RpcClient {
    private val counts = ConcurrentHashMap<String, AtomicInteger>()

    override suspend fun unaryCall(
        method: RpcMethodSpecifier,
        headers: Map<String, String>,
        request: ByteArray
    ): RpcResponse {
        val attempt = counts.getOrPut(method.methodName) { AtomicInteger(0) }.incrementAndGet()
        onUnary(method, attempt)
        return delegate.unaryCall(method, headers, request)
    }

    override suspend fun serverStreamingCall(
        method: RpcMethodSpecifier,
        block: suspend RpcServerStream.() -> Unit,
        readyCallback: () -> Unit
    ) = delegate.serverStreamingCall(method, block, readyCallback)

    companion object {
        fun rpcError(code: Codes, message: String = "injected fault") =
            RpcResponseException(path = "test", verb = "POST", code = code, errorMessage = message)
    }
}
