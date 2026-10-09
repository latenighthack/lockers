package com.latenighthack.lockers.connector
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.connector.storage.v1.*
import com.latenighthack.lockers.connector.internal.*
private val StoredAck.roomKey: ByteArray get() = roomIdRawValue
private val StoredAck.eventKey: ByteArray get() = eventIdRawValue
private val StoredAck.pendingKey: Int get() = if (confirmed) 1 else 0
/** V1 payload bytes remain unchanged; V2 adds only the pending query index. */
object SessionStoreImplDefinitionV2 : StoreDefinition<StoredAck>(StoreName("acks"), "StoredAck-protobuf-v1", StoredAck::fromByteArray, StoredAck::toByteArray) {
    val roomIdKey = bytesIndex(IndexName("roomIdRawValuebyteArrayIdentity"), StoredAck::roomKey, "byteArrayIdentity-v1")
    val eventIdKey = bytesIndex(IndexName("eventIdRawValuebyteArrayIdentity"), StoredAck::eventKey, "byteArrayIdentity-v1")
    val roomIdEventIdKey = compositeIndex(IndexName("composite_roomIdRawValuebyteArrayIdentity_eventIdRawValuebyteArrayIdentity"), roomIdKey, eventIdKey).also { primaryKey(it) }
    val confirmed = integerIndex(IndexName("confirmed_v2"), StoredAck::pendingKey)
}
data class AckConfirmationAge(val room: ByteArray, val event: ByteArray, val at: ByteArray)
object AckConfirmationAgeDefinitionV1 : StoreDefinition<AckConfirmationAge>(StoreName("ack_confirmation_age"), "AckConfirmationAge-frames-v1",
    { decodeFrames(it).let { fields -> require(fields.size == 3); AckConfirmationAge(fields[0], fields[1], fields[2]) } },
    { encodeFrames(it.room, it.event, it.at) }) {
    val room = bytesIndex(IndexName("room"), AckConfirmationAge::room, "bytes-v1")
    val event = bytesIndex(IndexName("event"), AckConfirmationAge::event, "bytes-v1")
    val at = bytesIndex(IndexName("confirmed_at"), AckConfirmationAge::at, "unsigned-big-endian-i64-v1")
    val identity = compositeIndex(IndexName("room_event"), room, event).also { primaryKey(it) }
}
internal class AckConfirmationAges(database: Database) : Store<AckConfirmationAge>(database, AckConfirmationAgeDefinitionV1) {
    suspend fun put(age: AckConfirmationAge) = save(age)
    suspend fun markIfMissing(age: AckConfirmationAge) {
        val relation = AckConfirmationAgeDefinitionV1.identity.eq(listOf(BoundStoreKey.SerializedKey(AckConfirmationAgeDefinitionV1.room.name.value, age.room), BoundStoreKey.SerializedKey(AckConfirmationAgeDefinitionV1.event.name.value, age.event)))
        if (get(relation) == null) save(age)
    }
    suspend fun remove(query: StoreRelation) = delete(query)
}
