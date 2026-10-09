package com.latenighthack.lockers.server.services.push.v1

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.storage.v2.*

private val ServerPushWork.pendingCreatedStorage: ByteArray? get() = if (completedAt == 0L && parkedReason.isEmpty()) OrderedKeyEncoding.long(createdAt) else null
private val ServerPushWork.backendStorage: ByteArray get() = OrderedKeyEncoding.int(if (backend in 1..3) backend else 0)
private val ServerPushWork.deadSessionStorage: ByteArray get() = pushSessionPrefix(sessionId) + pushId

/** Declared schemas stay frozen: retention and dead-letter indexes are additive. */
object PushRetentionDefinitionV3 : StoreDefinition<ServerPushWork>(
    StoreName("push_retention_v3"), "ServerPushWork-protobuf-v2", ServerPushWork.Companion::fromByteArray, ServerPushWork::toByteArray,
) {
    val id = bytesIndex(IndexName("push_id_raw"), ServerPushWork::pushId, "raw-bytes-v2").also { primaryKey(it) }
    val pendingCreated = nullableBytesIndex(IndexName("pending_created_ordered"), ServerPushWork::pendingCreatedStorage, "ordered-pending-created-v3")
}

object PushDeadLetterIndexDefinitionV2 : StoreDefinition<ServerPushWork>(
    StoreName("push_deadletter_index_v2"), "ServerPushWork-protobuf-v2", ServerPushWork.Companion::fromByteArray, ServerPushWork::toByteArray,
) {
    val id = bytesIndex(IndexName("push_id_raw"), ServerPushWork::pushId, "raw-bytes-v2").also { primaryKey(it) }
    val backend = bytesIndex(IndexName("backend_ordered"), ServerPushWork::backendStorage, "ordered-backend-v2")
    val session = bytesIndex(IndexName("session_push_ordered"), ServerPushWork::deadSessionStorage, "length-prefix-session-push-v2")
}

/** Automatic retry ends before completion deduplication can expire. Parked work is never age-pruned. */
data class PushRetentionPolicy(
    val retryWindowMs: Long = 30L * 24 * 60 * 60 * 1000,
    val completedRetentionMs: Long = 31L * 24 * 60 * 60 * 1000,
    val retainedGlobal: Long = 1_000_000,
) {
    init { require(retryWindowMs > 0 && completedRetentionMs > retryWindowMs && retainedGlobal > 0) }
}
