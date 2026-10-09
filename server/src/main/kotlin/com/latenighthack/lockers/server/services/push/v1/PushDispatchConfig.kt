package com.latenighthack.lockers.server.services.push.v1

/**
 * Runtime knobs for the push dispatcher.
 *
 * - [retryPolicy] governs transient-failure retries before dead-lettering.
 * - [sendConcurrencyPerBackend] caps how many sends run at once for a single
 *   backend, so a backlog burst can't open thousands of concurrent APNS/FCM/web
 *   calls (and one backend can't starve the others).
 * - [workerEnabled] gates dispatch; enabled replicas coordinate through durable leased claims.
 * - [sendTimeoutMs] bounds one cooperative provider call before its durable retry budget advances.
 * - [adminToken], when set, is required (as the `x-admin-token` header) on every
 *   PushAdmin call — defense in depth on top of binding admin to an internal port.
 */
data class PushDispatchConfig(
    val retryPolicy: PushRetryPolicy = PushRetryPolicy.DEFAULT,
    val sendConcurrencyPerBackend: Int = DEFAULT_SEND_CONCURRENCY,
    val workerEnabled: Boolean = true,
    val adminToken: String? = null,
    val sendTimeoutMs: Long = 30_000,
) {
    init { require(sendTimeoutMs > 0 && sendConcurrencyPerBackend in 1..256) }
    companion object {
        const val DEFAULT_SEND_CONCURRENCY = 32
        val DEFAULT = PushDispatchConfig()
    }
}
