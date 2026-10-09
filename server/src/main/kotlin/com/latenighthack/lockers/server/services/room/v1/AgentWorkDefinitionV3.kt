package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.ktstore.IndexName
import com.latenighthack.ktstore.OrderedKeyEncoding
import com.latenighthack.ktstore.StoreDefinition
import com.latenighthack.ktstore.StoreName
import com.latenighthack.lockers.server.storage.v2.ServerAgentWork
import com.latenighthack.lockers.server.storage.v2.fromByteArray
import com.latenighthack.lockers.server.storage.v2.toByteArray

private val ServerAgentWork.roomOrder: ByteArray get() = agentRoomPrefix(roomId) +
    OrderedKeyEncoding.long(sourceOrder) + writeRequestId
private val ServerAgentWork.activeOrder: ByteArray? get() = if (!AgentWorkState.terminal(state)) roomOrder else null
private val ServerAgentWork.dueOrder: ByteArray? get() =
    when (state) {
        AgentWorkState.PENDING, AgentWorkState.READY -> OrderedKeyEncoding.long(leaseUntil)
        AgentWorkState.RUNNING -> OrderedKeyEncoding.long(leaseUntil)
        else -> null
    }
private val ServerAgentWork.completedOrder: ByteArray? get() =
    if (AgentWorkState.terminal(
            state,
        )
    ) {
        OrderedKeyEncoding.long(completedAt)
    } else {
        null
    }

private val ServerAgentWork.pendingAge: ByteArray? get() =
    if (!AgentWorkState.terminal(
            state,
        )
    ) {
        OrderedKeyEncoding.long(if (encodedRequest.isEmpty()) 0 else createdAt)
    } else {
        null
    }

object AgentWorkDefinitionV3 : StoreDefinition<ServerAgentWork>(
    StoreName("agent_work_v2"),
    "ServerAgentWork-protobuf-v2",
    ServerAgentWork.Companion::fromByteArray,
    ServerAgentWork::toByteArray,
) {
    val id = bytesIndex(IndexName("work_key"), ServerAgentWork::key, "raw-work-key-v2").also { primaryKey(it) }
    val room = bytesIndex(IndexName("room_source_ordered"), ServerAgentWork::roomOrder, "room-source-write-v2")
    val active = nullableBytesIndex(
        IndexName("active_room_source_ordered"),
        ServerAgentWork::activeOrder,
        "active-room-source-write-v2",
    )
    val due = nullableBytesIndex(IndexName("due_ordered"), ServerAgentWork::dueOrder, "ordered-due-v2")
    val completed = nullableBytesIndex(
        IndexName("completed_ordered"),
        ServerAgentWork::completedOrder,
        "ordered-completed-v2",
    )
    val age = nullableBytesIndex(IndexName("pending_age"), ServerAgentWork::pendingAge, "ordered-time-v3")
}
