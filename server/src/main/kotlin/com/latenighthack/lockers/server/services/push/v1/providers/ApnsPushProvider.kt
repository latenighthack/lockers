package com.latenighthack.lockers.server.services.push.v1.providers

import com.eatthepath.pushy.apns.ApnsClient
import com.eatthepath.pushy.apns.ApnsClientBuilder
import com.eatthepath.pushy.apns.auth.ApnsSigningKey
import com.eatthepath.pushy.apns.util.SimpleApnsPayloadBuilder
import com.eatthepath.pushy.apns.util.SimpleApnsPushNotification
import com.eatthepath.pushy.apns.util.TokenUtil
import com.latenighthack.lockers.common.v1.Push
import com.latenighthack.lockers.push.v1.PushRegistration
import com.latenighthack.lockers.server.ApnsConfig
import kotlinx.coroutines.future.await
import org.slf4j.LoggerFactory
import java.io.File

/**
 * Delivers to Apple devices via pushy. The signing credentials are shared, but a
 * registration picks the sandbox or production APNS host, so a client on either
 * environment is served by a lazily-built client for that host.
 */
class ApnsPushProvider(private val config: ApnsConfig) : PushProvider {
    private val logger = LoggerFactory.getLogger(ApnsPushProvider::class.java)

    override val backend = PushBackendKind.APNS
    override val isConfigured: Boolean get() = config.isConfigured

    private val productionClient by lazy { buildClient(production = true) }
    private val developmentClient by lazy { buildClient(production = false) }

    private fun buildClient(production: Boolean): ApnsClient? {
        if (!config.isConfigured) {
            logger.warn(
                "APNS credentials not configured; Apple push disabled. " +
                    "Set APNS_TEAM_ID, APNS_KEY_ID and APNS_KEY_PATH."
            )
            return null
        }
        val keyFile = File(config.keyPath!!)
        if (!keyFile.exists()) {
            logger.error("APNS key file not found at: ${config.keyPath}")
            return null
        }
        return try {
            val signingKey = ApnsSigningKey.loadFromPkcs8File(keyFile, config.teamId, config.keyId)
            val host = if (production) ApnsClientBuilder.PRODUCTION_APNS_HOST else ApnsClientBuilder.DEVELOPMENT_APNS_HOST
            ApnsClientBuilder()
                .setApnsServer(host)
                .setSigningKey(signingKey)
                .build()
                .also { logger.info("APNS client initialized (production=$production)") }
        } catch (e: Exception) {
            logger.error("Failed to initialize APNS client (production=$production)", e)
            null
        }
    }

    override suspend fun send(registration: PushRegistration, push: Push): PushResult {
        val apns = registration.backend?.let { (it as? PushRegistration.OneOfBackend.apns)?.value }
            ?: return PushResult.Rejected("registration is not APNS", tokenInvalid = false)

        if (apns.deviceToken.isEmpty()) {
            return PushResult.Rejected("empty APNS device token", tokenInvalid = true)
        }

        val client = (if (apns.production) productionClient else developmentClient)
            ?: return PushResult.Retryable("APNS client unavailable")

        val token = TokenUtil.sanitizeTokenString(apns.deviceToken.decodeToString())
        val topic = apns.topic.ifEmpty { config.topic }

        val payloadBuilder = SimpleApnsPayloadBuilder()
        if (push.title.isNotEmpty()) payloadBuilder.setAlertTitle(push.title)
        if (push.body.isNotEmpty()) payloadBuilder.setAlertBody(push.body)
        val notification = SimpleApnsPushNotification(token, topic, payloadBuilder.build())

        return try {
            val response = client.sendNotification(notification).await()
            when {
                response.isAccepted -> PushResult.Accepted
                else -> PushResult.Rejected(
                    reason = response.rejectionReason.orElse("rejected"),
                    tokenInvalid = response.tokenInvalidationTimestamp.isPresent,
                )
            }
        } catch (e: Exception) {
            PushResult.Retryable(e.message ?: e::class.simpleName ?: "APNS send failed")
        }
    }

    override fun close() {
        runCatching { if (config.isConfigured) productionClient?.close() }
        runCatching { if (config.isConfigured) developmentClient?.close() }
    }
}
