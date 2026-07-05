package com.latenighthack.lockers.keymaster

import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktbuf.net.RpcMethodSpecifier
import com.latenighthack.ktbuf.net.RpcResponse
import com.latenighthack.ktbuf.net.RpcServerStream
import com.latenighthack.ktbuf.rpc.HttpRpcClient
import com.latenighthack.lockers.broadcast.v1.BroadcastAdminServiceRpc
import com.latenighthack.lockers.push.v1.PushAdminServiceRpc

private const val ADMIN_TOKEN_HEADER = "x-admin-token"

/**
 * ktbuf's [HttpRpcClient] re-adds the scheme itself: it prepends `http://` unless
 * the path already starts with `https`. So it wants a schemeless `host:port` for
 * plain HTTP, and a full `https://host:port` for TLS. Normalize a user-supplied
 * `--admin-url` (which naturally carries a scheme) to that contract.
 */
private fun serverPath(url: String): String =
    if (url.startsWith("https://")) url else url.removePrefix("http://")

/**
 * Injects fixed HTTP headers onto every unary call. The generated `*ServiceRpc`
 * stubs hardcode the unary headers to empty and expose only a query-parameter hook,
 * but the server checks the admin token in request *headers*
 * (`ApplicationCall.toGrpcRequestContext` maps HTTP headers to `context.headers`,
 * query params to `context.query`). Sending it as a header is both correct and keeps
 * the secret out of the URL and the server's access logs.
 */
private class HeaderInjectingRpcClient(
    private val delegate: RpcClient,
    private val extraHeaders: Map<String, String>,
) : RpcClient {
    override suspend fun unaryCall(
        method: RpcMethodSpecifier,
        headers: Map<String, String>,
        request: ByteArray,
    ): RpcResponse = delegate.unaryCall(method, headers + extraHeaders, request)

    override suspend fun serverStreamingCall(
        method: RpcMethodSpecifier,
        block: suspend RpcServerStream.() -> Unit,
        readyCallback: () -> Unit,
    ) = delegate.serverStreamingCall(method, block, readyCallback)
}

private fun KeymasterConfig.adminRpc(): RpcClient {
    val http = HttpRpcClient(serverPath(adminUrl))
    val token = adminToken ?: return http
    return HeaderInjectingRpcClient(http, mapOf(ADMIN_TOKEN_HEADER to token))
}

fun KeymasterConfig.broadcastAdmin(): BroadcastAdminServiceRpc = BroadcastAdminServiceRpc(adminRpc())

fun KeymasterConfig.pushAdmin(): PushAdminServiceRpc = PushAdminServiceRpc(adminRpc())
