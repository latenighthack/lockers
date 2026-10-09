package com.latenighthack.lockers.connector

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.connector.internal.*

/** A tombstone with empty registration bytes retains its monotonic revision after revocation. */
data class PushRegistrationIntent(val backend: Int, val revision: Long, val encodedRegistration: ByteArray, val pending: Boolean = true)
private val PushRegistrationIntent.backendKey: ByteArray get() = intToBytes(backend)
private fun encodeIntent(value: PushRegistrationIntent) = encodeFrames(intToBytes(value.backend), longBytes(value.revision), value.encodedRegistration, byteArrayOf(if (value.pending) 1 else 0))
private fun decodeIntent(bytes: ByteArray): PushRegistrationIntent {
    val fields = decodeFrames(bytes); require(fields.size == 4 && fields[0].size == 4 && fields[3].size == 1)
    val backend = fields[0].fold(0) { value, byte -> (value shl 8) or (byte.toInt() and 255) }
    return PushRegistrationIntent(backend, bytesLong(fields[1]), fields[2], fields[3][0] != 0.toByte())
}
object PushIntentDefinitionV2 : StoreDefinition<PushRegistrationIntent>(StoreName("push_intents"), "PushIntent-frames-v2", ::decodeIntent, ::encodeIntent) {
    val backend = bytesIndex(IndexName("backend"), PushRegistrationIntent::backendKey, "i32-big-endian-v1").also { primaryKey(it) }
}
internal class PushIntentStore(database: Database) : Store<PushRegistrationIntent>(database, PushIntentDefinitionV2) {
    suspend fun intents(): List<PushRegistrationIntent> { prepare(); return getAll().map { it.copy(encodedRegistration = it.encodedRegistration.copyOf()) } }
    suspend fun intent(backend: Int): PushRegistrationIntent? { prepare(); return get(PushIntentDefinitionV2.backend.eq(intToBytes(backend)))?.let { it.copy(encodedRegistration = it.encodedRegistration.copyOf()) } }
    suspend fun put(intent: PushRegistrationIntent) { prepare(); save(intent.copy(encodedRegistration = intent.encodedRegistration.copyOf())) }
}
