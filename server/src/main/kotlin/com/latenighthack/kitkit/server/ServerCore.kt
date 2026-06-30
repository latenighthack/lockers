package com.latenighthack.kitkit.server

import com.latenighthack.kitkit.server.services.greeter.v1.GreetingStore
import com.latenighthack.kitkit.server.services.greeter.v1.InMemoryGreetingStore
import me.tatarka.inject.annotations.Component
import me.tatarka.inject.annotations.Provides
import me.tatarka.inject.annotations.Scope

@Scope
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION, AnnotationTarget.PROPERTY_GETTER)
annotation class ServerCoreScope

/**
 * The root of the dependency-injection graph, shared by every service module.
 * Anything `@Provides`-ed here is injectable into any service's constructor.
 * In a real deployment these would be backed by ktstore-backed stores, blob
 * storage, metrics, etc. Here we expose a single in-memory [GreetingStore] so
 * the example has one concrete dependency to consume.
 */
@ServerCoreScope
@Component
abstract class ServerCore {
    @get:Provides
    val greetingStore: GreetingStore = InMemoryGreetingStore()
}
