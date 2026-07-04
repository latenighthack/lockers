package com.latenighthack.lockers.server.services.push.v1.providers

import com.google.auth.oauth2.GoogleCredentials
import com.google.firebase.FirebaseApp
import com.google.firebase.FirebaseOptions
import com.google.firebase.messaging.FirebaseMessaging
import com.google.firebase.messaging.FirebaseMessagingException
import com.google.firebase.messaging.MessagingErrorCode
import com.google.firebase.messaging.Message
import com.google.firebase.messaging.Notification
import com.latenighthack.lockers.common.v1.Push
import com.latenighthack.lockers.push.v1.PushRegistration
import com.latenighthack.lockers.server.FcmConfig
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.io.FileInputStream

/**
 * Delivers to Android devices via the Firebase Admin SDK. `FirebaseApp` is a
 * JVM-global singleton, so the app is initialized once under a private name and
 * reused. The blocking `send` runs on [Dispatchers.IO].
 */
class FcmPushProvider(private val config: FcmConfig) : PushProvider {
    private val logger = LoggerFactory.getLogger(FcmPushProvider::class.java)

    override val backend = PushBackendKind.FCM
    override val isConfigured: Boolean get() = config.isConfigured

    private val messaging: FirebaseMessaging? by lazy { initMessaging() }

    private fun initMessaging(): FirebaseMessaging? {
        if (!config.isConfigured) {
            logger.warn("FCM credentials not configured; Android push disabled. Set FCM_CREDENTIALS_PATH.")
            return null
        }
        return try {
            val existing = FirebaseApp.getApps().firstOrNull { it.name == APP_NAME }
            val app = existing ?: FileInputStream(config.credentialsPath!!).use { stream ->
                val options = FirebaseOptions.builder()
                    .setCredentials(GoogleCredentials.fromStream(stream))
                    .build()
                FirebaseApp.initializeApp(options, APP_NAME)
            }
            FirebaseMessaging.getInstance(app).also { logger.info("FCM messaging initialized") }
        } catch (e: Exception) {
            logger.error("Failed to initialize FCM", e)
            null
        }
    }

    override suspend fun send(registration: PushRegistration, push: Push): PushResult {
        val fcm = registration.backend?.let { (it as? PushRegistration.OneOfBackend.fcm)?.value }
            ?: return PushResult.Rejected("registration is not FCM", tokenInvalid = false)

        if (fcm.registrationToken.isEmpty()) {
            return PushResult.Rejected("empty FCM registration token", tokenInvalid = true)
        }

        val client = messaging ?: return PushResult.Retryable("FCM client unavailable")

        val message = Message.builder()
            .setToken(fcm.registrationToken)
            .setNotification(
                Notification.builder()
                    .setTitle(push.title.ifEmpty { null })
                    .setBody(push.body.ifEmpty { null })
                    .build()
            )
            .build()

        return withContext(Dispatchers.IO) {
            try {
                client.send(message)
                PushResult.Accepted
            } catch (e: FirebaseMessagingException) {
                mapError(e)
            } catch (e: Exception) {
                PushResult.Retryable(e.message ?: "FCM send failed")
            }
        }
    }

    private fun mapError(e: FirebaseMessagingException): PushResult = when (e.messagingErrorCode) {
        MessagingErrorCode.UNREGISTERED,
        MessagingErrorCode.SENDER_ID_MISMATCH ->
            PushResult.Rejected(e.messagingErrorCode.name, tokenInvalid = true)

        MessagingErrorCode.INVALID_ARGUMENT,
        MessagingErrorCode.THIRD_PARTY_AUTH_ERROR ->
            PushResult.Rejected(e.messagingErrorCode.name, tokenInvalid = false)

        MessagingErrorCode.UNAVAILABLE,
        MessagingErrorCode.INTERNAL,
        MessagingErrorCode.QUOTA_EXCEEDED ->
            PushResult.Retryable(e.messagingErrorCode.name)

        null -> PushResult.Retryable(e.message ?: "FCM error")
    }

    companion object {
        private const val APP_NAME = "lockers-push"
    }
}
