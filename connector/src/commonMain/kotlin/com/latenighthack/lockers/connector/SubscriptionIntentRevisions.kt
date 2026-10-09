package com.latenighthack.lockers.connector

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.connector.internal.*

/** Durable removal history. A room revision never resets while the session may remain live. */
data class SubscriptionIntentRevision(val room: ByteArray, val revision: Long, val subscribed: Boolean)
private fun encodeRevision(value: SubscriptionIntentRevision): ByteArray {
    require(value.room.size in 1..128 && value.revision > 0)
    return encodeFrames(value.room, longBytes(value.revision), byteArrayOf(if (value.subscribed) 1 else 0))
}
private fun decodeRevision(bytes: ByteArray): SubscriptionIntentRevision {
    val frames = decodeFrames(bytes); require(frames.size == 3 && frames[0].size in 1..128 && frames[2].size == 1 && frames[2][0] in 0..1)
    return SubscriptionIntentRevision(frames[0], bytesLong(frames[1]).also { require(it > 0) }, frames[2][0] == 1.toByte())
}
object SubscriptionIntentRevisionDefinitionV1 : StoreDefinition<SubscriptionIntentRevision>(StoreName("subscription_intent_revisions_v1"),
    "SubscriptionIntentRevision-frames-v1", ::decodeRevision, ::encodeRevision) {
    val room = bytesIndex(IndexName("room"), SubscriptionIntentRevision::room, "room-raw-bytes-v1").also { primaryKey(it) }
}
internal class SubscriptionIntentRevisions(private val database: Database, private val maximum: Int) : Store<SubscriptionIntentRevision>(database, SubscriptionIntentRevisionDefinitionV1) {
    suspend fun find(room: ByteArray): SubscriptionIntentRevision? = get(SubscriptionIntentRevisionDefinitionV1.room.eq(room))?.let { decodeRevision(encodeRevision(it)) }
    suspend fun put(value: SubscriptionIntentRevision) {
        if (find(value.room) == null && database.count(SubscriptionIntentRevisionDefinitionV1.storeName, SubscriptionIntentRevisionDefinitionV1.room.query(1)) >= maximum)
            throw SubscriptionHistoryCapacityException()
        save(decodeRevision(encodeRevision(value)))
    }
}
class SubscriptionHistoryCapacityException : IllegalArgumentException("Subscription intent history namespace exhausted; trusted storage maintenance is required")
class SubscriptionOrderingUnsupportedException : IllegalArgumentException("Server does not support ordered subscription intents")
class SubscriptionRevisionConflictException(val currentRevision: Long) : IllegalArgumentException("Subscription revision conflicts require an explicit caller retry")
internal class SubscriptionIntentStaleException(val currentRevision: Long) : IllegalArgumentException("Subscription intent is stale")
