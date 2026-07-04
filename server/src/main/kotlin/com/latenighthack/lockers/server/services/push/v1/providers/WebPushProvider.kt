package com.latenighthack.lockers.server.services.push.v1.providers

import com.interaso.webpush.VapidKeys
import com.interaso.webpush.WebPush
import com.interaso.webpush.WebPushException
import com.interaso.webpush.WebPushService
import com.latenighthack.lockers.common.v1.Push
import com.latenighthack.lockers.push.v1.PushRegistration
import com.latenighthack.lockers.server.WebPushConfig
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory

/**
 * Delivers to browsers via the Web Push protocol (RFC 8291 payload encryption +
 * VAPID), using the pure-Kotlin `com.interaso:webpush` library (JDK crypto, no
 * BouncyCastle). The blocking `send` runs on [Dispatchers.IO].
 */
class WebPushProvider(private val config: WebPushConfig) : PushProvider {
    private val logger = LoggerFactory.getLogger(WebPushProvider::class.java)

    override val backend = PushBackendKind.WEB_PUSH
    override val isConfigured: Boolean get() = config.isConfigured

    private val vapidKeys: VapidKeys? by lazy {
        if (!config.isConfigured) {
            logger.warn(
                "Web Push VAPID keys not configured; web push disabled. " +
                    "Set WEBPUSH_VAPID_PUBLIC_KEY, WEBPUSH_VAPID_PRIVATE_KEY and WEBPUSH_SUBJECT."
            )
            return@lazy null
        }
        try {
            VapidKeys.Factory.fromUncompressedBytes(config.vapidPublicKey!!, config.vapidPrivateKey!!)
        } catch (e: Exception) {
            logger.error("Failed to load VAPID keys", e)
            null
        }
    }

    private val service: WebPushService? by lazy {
        vapidKeys?.let { WebPushService(config.subject!!, it) }
    }

    /** The 65-byte uncompressed VAPID public key browsers need to subscribe, or null when unconfigured. */
    val applicationServerKey: ByteArray? get() = vapidKeys?.applicationServerKey

    override suspend fun send(registration: PushRegistration, push: Push): PushResult {
        val web = registration.backend?.let { (it as? PushRegistration.OneOfBackend.webPush)?.value }
            ?: return PushResult.Rejected("registration is not web push", tokenInvalid = false)

        if (web.endpoint.isEmpty()) {
            return PushResult.Rejected("empty web push endpoint", tokenInvalid = true)
        }

        val client = service ?: return PushResult.Retryable("web push service unavailable")
        val payload = encodePayload(push).encodeToByteArray()

        return withContext(Dispatchers.IO) {
            try {
                when (client.send(payload, web.endpoint, web.p256Dh, web.auth)) {
                    WebPush.SubscriptionState.ACTIVE -> PushResult.Accepted
                    WebPush.SubscriptionState.EXPIRED -> PushResult.Rejected("subscription expired", tokenInvalid = true)
                }
            } catch (e: WebPushException) {
                PushResult.Retryable(e.message ?: "web push send failed")
            } catch (e: Exception) {
                PushResult.Retryable(e.message ?: "web push send failed")
            }
        }
    }

    private fun encodePayload(push: Push): String =
        """{"title":${jsonString(push.title)},"body":${jsonString(push.body)}}"""

    private fun jsonString(value: String): String {
        val sb = StringBuilder(value.length + 2)
        sb.append('"')
        for (c in value) {
            when (c) {
                '"' -> sb.append("\\\"")
                '\\' -> sb.append("\\\\")
                '\n' -> sb.append("\\n")
                '\r' -> sb.append("\\r")
                '\t' -> sb.append("\\t")
                else -> if (c < ' ') sb.append("\\u%04x".format(c.code)) else sb.append(c)
            }
        }
        sb.append('"')
        return sb.toString()
    }
}
