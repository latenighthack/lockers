package com.latenighthack.lockers.connector.internal

import com.latenighthack.ktbuf.bytes.toBase64String
import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.lockers.room.v1.*

internal class ShardedRoomServiceRpc(rpcClient: RpcClient): RoomServiceRpc(rpcClient, { _, request ->
    val roomId = when (request) {
        is SubscriptionRequest -> request.roomId
        is GetLockerRequest -> request.roomId
        is GetAllLockersRequest -> request.roomId
        is PostLockerChangeRequest -> request.roomId
        else -> null
    }

    roomId?.let { mapOf(Pair("rid", roomId.rawValue.toBase64String())) } ?: emptyMap()
})
