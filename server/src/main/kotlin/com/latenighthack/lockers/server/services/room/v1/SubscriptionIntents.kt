package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.ktstore.*
import com.latenighthack.ktbuf.net.RpcResponseException
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.lockers.server.ServerResourceLimits
import com.latenighthack.lockers.server.invalidArgument
import com.latenighthack.lockers.server.storage.v1.*
import java.io.*
import kotlinx.coroutines.ensureActive

data class SubscriptionIntent(val session: ByteArray, val room: ByteArray, val revision: Long, val subscribed: Boolean)
data class SubscriptionIntentResult<T>(val currentRevision: Long, val stale: Boolean, val value: T? = null)
private fun encodeIntent(value: SubscriptionIntent): ByteArray = ByteArrayOutputStream().use { buffer ->
    DataOutputStream(buffer).use { out ->
        require(value.session.size in 1..128 && value.room.size in 1..128 && value.revision > 0)
        out.writeInt(value.session.size); out.write(value.session); out.writeInt(value.room.size); out.write(value.room)
        out.writeLong(value.revision); out.writeBoolean(value.subscribed)
    }; buffer.toByteArray()
}
private fun decodeIntent(bytes: ByteArray): SubscriptionIntent = DataInputStream(ByteArrayInputStream(bytes)).use { input ->
    fun bytes(): ByteArray { val size = input.readInt(); require(size in 1..128 && size <= input.available()); return ByteArray(size).also(input::readFully) }
    val sid = bytes(); val room = bytes(); val revision = input.readLong(); val subscribed = input.readUnsignedByte()
    require(subscribed in 0..1)
    val value = SubscriptionIntent(sid, room, revision, subscribed == 1)
    require(value.revision > 0 && input.available() == 0); value
}
/** Additive authority ledger: unsubscribe rows remain until the session is permanently destroyed. */
object SubscriptionIntentDefinitionV2 : StoreDefinition<SubscriptionIntent>(StoreName("subscription_intents_v2"),
    "subscription-intent-raw-identities-v2", ::decodeIntent, ::encodeIntent) {
    val session = bytesIndex(IndexName("session"), SubscriptionIntent::session, "raw-session-v2")
    val room = bytesIndex(IndexName("room"), SubscriptionIntent::room, "raw-room-v2")
    val identity = compositeIndex(IndexName("identity"), session, room).also { primaryKey(it) }
}
class SubscriptionIntents(private val database: Database, private val limits: ServerResourceLimits) : Store<SubscriptionIntent>(database, SubscriptionIntentDefinitionV2) {
    private fun key(session: ServerSessionId, room: ServerRoomId) = SubscriptionIntentDefinitionV2.identity.eq(listOf(
        BoundStoreKey.SerializedKey(SubscriptionIntentDefinitionV2.session.name.value, session.rawValue),
        BoundStoreKey.SerializedKey(SubscriptionIntentDefinitionV2.room.name.value, room.rawValue)))
    private fun conflict(message: String): Nothing = throw RpcResponseException("", "RPC", Codes.FAILED_PRECONDITION, message)
    suspend fun <T> apply(session: ServerSessionId, room: ServerRoomId, revision: Long, subscribed: Boolean, mutation: suspend () -> T): SubscriptionIntentResult<T> =
        database.transaction("lockers.session-authority") { database.transaction(roomMutationKey(room)) { database.transaction("lockers.subscription-admission") admission@{
            prepare()
            if (revision < 0) invalidArgument("Negative subscription intent revision")
            val existing = get(key(session, room))
            val floor = existing?.revision ?: 0L
            if (revision < floor) return@admission SubscriptionIntentResult<T>(floor, true)
            if (revision > 0 && revision == floor && existing!!.subscribed != subscribed) conflict("Subscription intent revision reused for a different desired state")
            if (revision > floor) {
                if (existing == null) {
                    val definition = SubscriptionIntentDefinitionV2
                    val total = database.count(definition.storeName, definition.session.query(1))
                    val owned = database.count(definition.storeName, definition.session.query(1, lower = session.rawValue, upper = session.rawValue))
                    if (total >= limits.maxSubscriptionIntents || owned >= limits.maxSubscriptionIntentsPerSession)
                        conflict("Subscription intent identity namespace exhausted; replace the session or perform trusted namespace maintenance")
                }
                save(SubscriptionIntent(session.rawValue.copyOf(), room.rawValue.copyOf(), revision, subscribed))
            }
            SubscriptionIntentResult(revision, false, mutation())
        } } }
    /** Continuation pages cannot restore or change an intent; they observe exactly its active revision. */
    suspend fun <T> observe(session: ServerSessionId, room: ServerRoomId, revision: Long, mutation: suspend () -> T): SubscriptionIntentResult<T> =
        database.transaction("lockers.session-authority") {
            prepare()
            if (revision < 0) invalidArgument("Negative subscription intent revision")
            val existing = get(key(session, room)); val floor = existing?.revision ?: 0L
            if (revision != floor || (revision > 0 && existing?.subscribed != true)) SubscriptionIntentResult(floor, true)
            else SubscriptionIntentResult(floor, false, mutation())
        }
    suspend fun clearForSession(session: ServerSessionId) = database.transaction("lockers.session-authority") {
        prepare()
        while (database.deleteBatch(SubscriptionIntentDefinitionV2.storeName,
            SubscriptionIntentDefinitionV2.session.query(256, lower = session.rawValue, upper = session.rawValue)) > 0) kotlinx.coroutines.currentCoroutineContext().ensureActive()
    }
}
