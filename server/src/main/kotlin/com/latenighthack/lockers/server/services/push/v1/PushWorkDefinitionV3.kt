package com.latenighthack.lockers.server.services.push.v1

import com.latenighthack.ktstore.IndexName
import com.latenighthack.ktstore.OrderedKeyEncoding
import com.latenighthack.ktstore.StoreDefinition
import com.latenighthack.ktstore.StoreName
import com.latenighthack.lockers.server.storage.v2.ServerPushWork
import com.latenighthack.lockers.server.storage.v2.fromByteArray
import com.latenighthack.lockers.server.storage.v2.toByteArray

private val ServerPushWork.due: ByteArray get() =
    pushBackendTime(
        backend,
        if (completedAt != 0L || parkedReason.isNotEmpty()) Long.MAX_VALUE else maxOf(leaseUntil, retryAfter),
    )
private val ServerPushWork.sessionOrder: ByteArray get() = pushSessionPrefix(sessionId) + pushId
private val ServerPushWork.pendingSessionOrder: ByteArray? get() = if (completedAt == 0L &&
    parkedReason.isEmpty()
) {
    sessionOrder
} else {
    null
}
private val ServerPushWork.completedOrder: ByteArray? get() = completedAt.takeIf {
    it != 0L
}?.let(OrderedKeyEncoding::long)

/** Additive indexes: existing push queue schema remains unchanged. */
private val ServerPushWork.pendingAge: ByteArray? get() =
    if (completedAt == 0L &&
        parkedReason.isEmpty()
    ) {
        OrderedKeyEncoding.long(enqueuedAt)
    } else {
        null
    }
private val ServerPushWork.backendAge: ByteArray? get() = pendingAge?.let { pushBackendTime(backend, enqueuedAt) }
private val ServerPushWork.readyOrder: ByteArray? get() =
    if (pendingAge ==
        null
    ) {
        null
    } else {
        pushBackendTime(backend, maxOf(enqueuedAt, retryAfter, leaseUntil))
    }

object PushWorkDefinitionV3 : StoreDefinition<ServerPushWork>(
    StoreName("push_work_v2"),
    "ServerPushWork-protobuf-v2",
    ServerPushWork.Companion::fromByteArray,
    ServerPushWork::toByteArray,
) {
    val id = bytesIndex(IndexName("push_id_raw"), ServerPushWork::pushId, "raw-bytes-v2").also { primaryKey(it) }
    val due = bytesIndex(IndexName("backend_due_ordered"), ServerPushWork::due, "ordered-backend-time-v2")
    val session = bytesIndex(
        IndexName("session_push_ordered"),
        ServerPushWork::sessionOrder,
        "length-prefix-session-push-v2",
    )
    val pendingSession =
        nullableBytesIndex(
            IndexName("pending_session_push_ordered"),
            ServerPushWork::pendingSessionOrder,
            "length-prefix-session-push-v2",
        )
    val completed = nullableBytesIndex(
        IndexName("completed_at_ordered"),
        ServerPushWork::completedOrder,
        "ordered-time-v2",
    )
    val ready = nullableBytesIndex(IndexName("observed_ready"), ServerPushWork::readyOrder, "ordered-backend-time-v3")
    val age = nullableBytesIndex(IndexName("pending_age"), ServerPushWork::pendingAge, "ordered-time-v3")
    val backendAge = nullableBytesIndex(
        IndexName("backend_pending_age"),
        ServerPushWork::backendAge,
        "ordered-backend-time-v3",
    )
}
