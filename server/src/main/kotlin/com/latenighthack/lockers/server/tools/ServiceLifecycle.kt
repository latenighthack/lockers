package com.latenighthack.lockers.server.tools

import kotlinx.coroutines.asContextElement
import kotlinx.coroutines.runBlocking

/** Guards compatibility blocking adapters against waiting for their own child/dispatcher. */
internal object ServiceLifecycle {
    private val owned = ThreadLocal<Boolean>()
    val context = owned.asContextElement(true)
    fun requireExternalClose() {
        check(owned.get() != true) { "Close must be initiated by the external lifecycle owner, outside an in-flight service call" }
    }
    fun blockingClose(parentContext: kotlin.coroutines.CoroutineContext = kotlin.coroutines.EmptyCoroutineContext, block: suspend () -> Unit) {
        requireExternalClose()
        val dispatcher = parentContext[kotlin.coroutines.ContinuationInterceptor]
        check(dispatcher == null || dispatcher === kotlinx.coroutines.Dispatchers.Default || dispatcher === kotlinx.coroutines.Dispatchers.IO) {
            "Custom embedding dispatchers require suspend closeAndJoin; blocking shutdown may prevent child cleanup"
        }
        runBlocking { block() }
    }
}
