package com.latenighthack.lockers.server

import com.latenighthack.ktbuf.net.RpcResponseException
import com.latenighthack.ktbuf.proto.Codes
import com.latenighthack.lockers.common.v1.RoomId

/** Per-replica read work is finite even when a caller rotates untrusted room identities. */
class ReadAdmission(private val limits: ServerResourceLimits, private val nanoTime: () -> Long = System::nanoTime) {
    private class Bucket(var tokens: Double, var last: Long) {
        fun consume(now: Long, rate: Int, burst: Int): Boolean {
            tokens = minOf(burst.toDouble(), tokens + (now - last).coerceAtLeast(0) / 1_000_000_000.0 * rate)
            last = now
            if (tokens < 1) return false
            tokens -= 1
            return true
        }
    }
    private val global = Bucket(limits.globalReadBurst.toDouble(), nanoTime())
    private val rooms = java.util.LinkedHashMap<RoomId, Bucket>(16, 0.75f, true)
    internal val trackedRoomCount: Int get() = synchronized(this) { rooms.size }

    fun tryAcquire(room: RoomId): Boolean = synchronized(this) {
        val now = nanoTime()
        if (!global.consume(now, limits.globalReadsPerSecond, limits.globalReadBurst)) return@synchronized false
        val canonical = RoomId(room.rawValue.copyOf())
        val bucket = rooms[canonical] ?: Bucket(limits.roomReadBurst.toDouble(), now).also {
            if (rooms.size == limits.maxTrackedReadRooms) rooms.remove(rooms.keys.first())
            rooms[canonical] = it
        }
        bucket.consume(now, limits.roomReadsPerSecond, limits.roomReadBurst)
    }
    suspend fun require(room: RoomId) {
        if (!ProtocolValidation.identity(room.rawValue)) invalidArgument("Invalid room identity")
        if (!tryAcquire(room)) throw RpcResponseException("", "RPC", Codes.RESOURCE_EXHAUSTED, "Read admission exhausted")
        if (!ProtocolValidation.room(room)) invalidArgument("Invalid room authority")
    }
}

internal fun invalidArgument(message: String): Nothing = throw RpcResponseException("", "RPC", Codes.INVALID_ARGUMENT, message)
