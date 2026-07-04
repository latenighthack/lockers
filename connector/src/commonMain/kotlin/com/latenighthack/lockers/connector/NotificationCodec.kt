package com.latenighthack.lockers.connector

import com.latenighthack.lockers.common.v1.LockerId
import com.latenighthack.lockers.common.v1.LockerKeyspace
import com.latenighthack.lockers.common.v1.RoomId

/**
 * The context a [NotificationCodec] sees for one notification. [title]/[body] are
 * the OS-facing push strings (read-only here); the codec only rewrites the opaque
 * [payload][NotificationCodec] bytes.
 */
class NotificationContext(
    val roomId: RoomId,
    val lockerId: LockerId,
    val keyspace: LockerKeyspace,
    val title: String?,
    val body: String?,
)

/**
 * A consumer-supplied transform over a notification's opaque payload. [decode]
 * runs on the receive side as an event comes off the session channel; [encode]
 * runs on the send side when a write carries a notification. Codecs compose (see
 * [NotificationCodecs]); return `null` from [decode] to drop the notification.
 */
interface NotificationCodec {
    suspend fun decode(context: NotificationContext, payload: ByteArray): ByteArray?
    suspend fun encode(context: NotificationContext, payload: ByteArray): ByteArray = payload
}

/**
 * An ordered, composable set of [NotificationCodec]s, optionally scoped per
 * keyspace. On send the chain is applied front-to-back; on receive it is applied
 * back-to-front, so `[A, B]` wraps as `B(A(payload))` and unwraps symmetrically.
 * The default ([identity]) is an empty chain that passes payloads through
 * unchanged, preserving out-of-the-box behavior.
 */
class NotificationCodecs private constructor(
    private val global: List<NotificationCodec>,
    private val perKeyspace: Map<Long, List<NotificationCodec>>,
) {
    private fun chainFor(keyspace: LockerKeyspace): List<NotificationCodec> =
        perKeyspace[keyspace.value] ?: global

    /** True when no codec applies for [keyspace] (the fast path). */
    fun isEmpty(keyspace: LockerKeyspace): Boolean = chainFor(keyspace).isEmpty()

    /** Applies the receive-side chain; returns null if any codec drops the notification. */
    suspend fun decode(context: NotificationContext, payload: ByteArray): ByteArray? {
        var current = payload
        for (codec in chainFor(context.keyspace).asReversed()) {
            current = codec.decode(context, current) ?: return null
        }
        return current
    }

    /** Applies the send-side chain. */
    suspend fun encode(context: NotificationContext, payload: ByteArray): ByteArray {
        var current = payload
        for (codec in chainFor(context.keyspace)) {
            current = codec.encode(context, current)
        }
        return current
    }

    class Builder {
        private val global = mutableListOf<NotificationCodec>()
        private val perKeyspace = mutableMapOf<Long, MutableList<NotificationCodec>>()

        /** Adds a codec applied to every keyspace that has no specific chain. */
        fun add(codec: NotificationCodec) = apply { global += codec }

        /** Adds a codec scoped to [keyspace]; a keyspace with its own chain ignores the global one. */
        fun add(keyspace: LockerKeyspace, codec: NotificationCodec) = apply {
            perKeyspace.getOrPut(keyspace.value) { mutableListOf() } += codec
        }

        fun build(): NotificationCodecs =
            NotificationCodecs(global.toList(), perKeyspace.mapValues { it.value.toList() })
    }

    companion object {
        /** The pass-through default: payloads are delivered unchanged. */
        fun identity(): NotificationCodecs = NotificationCodecs(emptyList(), emptyMap())

        /** A global chain from the given codecs, applied to all keyspaces. */
        fun of(vararg codecs: NotificationCodec): NotificationCodecs =
            NotificationCodecs(codecs.toList(), emptyMap())

        fun builder(): Builder = Builder()
    }
}
