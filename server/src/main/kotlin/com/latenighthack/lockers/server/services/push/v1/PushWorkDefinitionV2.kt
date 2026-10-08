package com.latenighthack.lockers.server.services.push.v1

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v2.*
import java.nio.ByteBuffer

internal fun pushSessionPrefix(id: ByteArray) = ByteBuffer.allocate(4 + id.size).putInt(id.size).put(id).array()
internal fun pushBackendTime(backend: Int, time: Long) = OrderedKeyEncoding.int(if (backend in 1..3) backend else 0) + OrderedKeyEncoding.long(time)
private val ServerPushWork.due: ByteArray get() = pushBackendTime(backend,
    if (completedAt != 0L || parkedReason.isNotEmpty()) Long.MAX_VALUE else maxOf(leaseUntil, retryAfter))
private val ServerPushWork.sessionOrder: ByteArray get() = pushSessionPrefix(sessionId) + pushId
private val ServerPushWork.pendingSessionOrder: ByteArray? get() = if (completedAt == 0L && parkedReason.isEmpty()) sessionOrder else null
private val ServerPushWork.completedOrder: ByteArray? get() = completedAt.takeIf { it != 0L }?.let(OrderedKeyEncoding::long)

/** Additive indexes: existing push queue schema remains unchanged. */
object PushWorkDefinitionV2 : StoreDefinition<ServerPushWork>(
    StoreName("push_work_v2"), "ServerPushWork-protobuf-v2", ServerPushWork.Companion::fromByteArray, ServerPushWork::toByteArray,
) {
    val id = bytesIndex(IndexName("push_id_raw"), ServerPushWork::pushId, "raw-bytes-v2").also { primaryKey(it) }
    val due = bytesIndex(IndexName("backend_due_ordered"), ServerPushWork::due, "ordered-backend-time-v2")
    val session = bytesIndex(IndexName("session_push_ordered"), ServerPushWork::sessionOrder, "length-prefix-session-push-v2")
    val pendingSession = nullableBytesIndex(IndexName("pending_session_push_ordered"), ServerPushWork::pendingSessionOrder, "length-prefix-session-push-v2")
    val completed = nullableBytesIndex(IndexName("completed_at_ordered"), ServerPushWork::completedOrder, "ordered-time-v2")
}
