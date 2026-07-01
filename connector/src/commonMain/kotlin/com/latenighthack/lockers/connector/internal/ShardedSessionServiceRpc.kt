package com.latenighthack.lockers.connector.internal

import com.latenighthack.ktbuf.bytes.toBase64String
import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.lockers.session.v1.SessionServiceRpc
import com.latenighthack.lockers.session.v1.WatchSessionRequest

internal class ShardedSessionServiceRpc(rpcClient: RpcClient) : SessionServiceRpc(rpcClient, { _, request ->
    val sessionId = (request as? WatchSessionRequest)?.request?.getOpen()?.sessionId
        ?: (request as? WatchSessionRequest)?.request?.getCreate()?.sessionId

    if (sessionId != null) {
        mapOf(Pair("s", sessionId.rawValue.toBase64String()))
    } else {
        emptyMap()
    }
})
