package com.latenighthack.lockers.server.services.push.v1.providers

import com.latenighthack.lockers.server.LockersConfig

/** Builds the built-in provider set (one per backend) from [LockersConfig]. */
object PushProviders {
    fun fromConfig(config: LockersConfig): List<PushProvider> = listOf(
        ApnsPushProvider(config.apns),
        FcmPushProvider(config.fcm),
        WebPushProvider(config.webPush),
    )
}
