package com.latenighthack.lockers.connector

import com.latenighthack.lockers.common.v1.*

/** Session broadcasts have no locker identity or keyspace. Metadata is the actual wire metadata. */
data class BroadcastContext(val roomId: RoomId, val eventId: EventId, val title: String?, val body: String?)
interface BroadcastCodec {
    suspend fun decode(context: BroadcastContext, payload: ByteArray): ByteArray?
}
class BroadcastCodecs private constructor(private val codecs: List<BroadcastCodec>) {
    suspend fun decode(context: BroadcastContext, payload: ByteArray): ByteArray? {
        var decoded = payload
        for (codec in codecs.asReversed()) decoded = codec.decode(context, decoded) ?: return null
        return decoded
    }
    companion object {
        fun identity() = BroadcastCodecs(emptyList())
        fun of(vararg codecs: BroadcastCodec) = BroadcastCodecs(codecs.toList())
    }
}
data class IncomingBroadcast(val cursor: Long, val context: BroadcastContext, val payload: ByteArray)
