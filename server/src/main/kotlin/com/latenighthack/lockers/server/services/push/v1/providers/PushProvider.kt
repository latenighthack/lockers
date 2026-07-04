package com.latenighthack.lockers.server.services.push.v1.providers

import com.latenighthack.lockers.common.v1.Push
import com.latenighthack.lockers.push.v1.PushRegistration

/**
 * The delivery backends the push service can dispatch to. The [protoValue]
 * mirrors the `push.v1.PushBackend` enum numbers (fixed by the proto contract),
 * so a stored queue row — which carries the raw int — maps back to a provider.
 */
enum class PushBackendKind(val protoValue: Int) {
    APNS(1),
    FCM(2),
    WEB_PUSH(3);

    companion object {
        fun fromProtoValue(value: Int): PushBackendKind? = entries.firstOrNull { it.protoValue == value }

        /** The backend a registration carries, or null if the oneof is unset. */
        fun of(registration: PushRegistration): PushBackendKind? = when (registration.backend) {
            is PushRegistration.OneOfBackend.apns -> APNS
            is PushRegistration.OneOfBackend.fcm -> FCM
            is PushRegistration.OneOfBackend.webPush -> WEB_PUSH
            null -> null
        }
    }
}

/** The outcome of a single delivery attempt, driving the queue's retry decision. */
sealed interface PushResult {
    /** Delivered; clear the queue row. */
    data object Accepted : PushResult

    /**
     * Permanently refused. [tokenInvalid] means the credential is dead and its
     * registration should be dropped, not retried.
     */
    data class Rejected(val reason: String, val tokenInvalid: Boolean) : PushResult

    /** Transient failure (network / 5xx); retry with backoff. */
    data class Retryable(val reason: String) : PushResult
}

/**
 * A single delivery backend. Providers are stateless per call and own their
 * vendor client. An unconfigured provider ([isConfigured] false) is skipped
 * rather than failing, so a server with only some backends set up still runs.
 */
interface PushProvider {
    val backend: PushBackendKind
    val isConfigured: Boolean

    /** Delivers [push] to the device described by [registration]. */
    suspend fun send(registration: PushRegistration, push: Push): PushResult

    fun close() {}
}
