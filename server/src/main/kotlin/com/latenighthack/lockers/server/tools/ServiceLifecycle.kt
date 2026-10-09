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
    fun blockingClose(block: suspend () -> Unit) {
        requireExternalClose()
        runBlocking { block() }
    }
}
