package com.latenighthack.lockers.server.tools

import io.micrometer.core.instrument.MeterRegistry
import kotlin.reflect.KProperty

suspend fun <T, U : com.latenighthack.ktbuf.proto.Enum> MeterRegistry.trackResponse(key: String, resultProp: KProperty<U>, callback: suspend () -> T): T {
    val response = callback()
    val responseEnum = resultProp.getter.call(response)
    val responseString = responseEnum.toString()

    counter(key, "result", responseString).increment()

    return response
}
