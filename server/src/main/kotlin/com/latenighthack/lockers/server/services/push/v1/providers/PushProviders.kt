package com.latenighthack.lockers.server.services.push.v1.providers

import com.latenighthack.lockers.server.LockersConfig

/** Builds the built-in lazy provider set (one per backend), unwinding partial construction. */
object PushProviders {
    fun fromConfig(config: LockersConfig): List<PushProvider> = build(listOf(
        { ApnsPushProvider(config.apns) },
        { FcmPushProvider(config.fcm) },
        { WebPushProvider(config.webPush) },
    ))

    internal fun build(factories: List<() -> PushProvider>): List<PushProvider> {
        val created = mutableListOf<PushProvider>()
        try { factories.forEach { created.add(it()) }; return created }
        catch (failure: Throwable) {
            created.asReversed().forEach {
                try { it.close() } catch (cleanup: Throwable) { failure.addSuppressed(cleanup) }
            }
            throw failure
        }
    }
}
