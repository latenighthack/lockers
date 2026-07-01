package com.latenighthack.lockers.server

import com.latenighthack.ktstore.StoreDelegate
import com.latenighthack.lockers.server.agents.ExampleLockerAgent
import com.latenighthack.lockers.server.agents.LockerAgentRegistry
import com.latenighthack.lockers.server.services.push.v1.PushQueueStore
import com.latenighthack.lockers.server.services.push.v1.PushQueueStoreImpl
import com.latenighthack.lockers.server.services.push.v1.PushSessionStore
import com.latenighthack.lockers.server.services.push.v1.PushSessionStoreImpl
import com.latenighthack.lockers.server.services.room.v1.LockerStore
import com.latenighthack.lockers.server.services.room.v1.LockerStoreImpl
import com.latenighthack.lockers.server.services.room.v1.SubscriptionStore
import com.latenighthack.lockers.server.services.room.v1.SubscriptionStoreImpl
import com.latenighthack.lockers.server.services.session.v1.SessionInboxStore
import com.latenighthack.lockers.server.services.session.v1.SessionInboxStoreImpl
import com.latenighthack.lockers.server.services.session.v1.SessionStore
import com.latenighthack.lockers.server.services.session.v1.SessionStoreImpl
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import me.tatarka.inject.annotations.Component
import me.tatarka.inject.annotations.Provides
import me.tatarka.inject.annotations.Scope

@Scope
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION, AnnotationTarget.PROPERTY_GETTER)
annotation class ServerCoreScope

/**
 * The root of the dependency-injection graph, shared by every locker service
 * module. Everything `@Provides`-ed here is injectable into any service's
 * constructor. All state is backed by the injected [StoreDelegate] (in-memory in
 * tests, a persistent delegate in production).
 */
@ServerCoreScope
@Component
abstract class ServerCore(
    private val storageDelegate: StoreDelegate
) {
    private val sessionStoreImpl by lazy { SessionStoreImpl(storageDelegate) }
    private val sessionInboxStoreImpl by lazy { SessionInboxStoreImpl(storageDelegate) }
    private val subscriptionStoreImpl by lazy { SubscriptionStoreImpl(storageDelegate) }
    private val lockerStoreImpl by lazy { LockerStoreImpl(storageDelegate) }
    private val pushSessionStoreImpl by lazy { PushSessionStoreImpl(storageDelegate) }
    private val pushQueueStoreImpl by lazy { PushQueueStoreImpl(storageDelegate) }

    @get:Provides val sessionStore: SessionStore = sessionStoreImpl
    @get:Provides val sessionInboxStore: SessionInboxStore = sessionInboxStoreImpl
    @get:Provides val subscriptionStore: SubscriptionStore = subscriptionStoreImpl
    @get:Provides val lockerStore: LockerStore = lockerStoreImpl
    @get:Provides val pushStore: PushSessionStore = pushSessionStoreImpl
    @get:Provides val pushQueueStore: PushQueueStore = pushQueueStoreImpl

    var overrideMeterRegistry: MeterRegistry? = null
    private val _meterRegistry by lazy { overrideMeterRegistry ?: SimpleMeterRegistry() }
    @get:Provides val meterRegistry: MeterRegistry get() = _meterRegistry

    @get:Provides val agentRegistry: LockerAgentRegistry = ExampleLockerAgent()

    suspend fun setup() {
        sessionStoreImpl.prepare()
        sessionInboxStoreImpl.prepare()
        subscriptionStoreImpl.prepare()
        lockerStoreImpl.prepare()
        pushSessionStoreImpl.prepare()
        pushQueueStoreImpl.prepare()

        storageDelegate.createStores()
    }
}
