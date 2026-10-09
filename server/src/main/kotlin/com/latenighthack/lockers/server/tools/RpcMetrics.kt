package com.latenighthack.lockers.server.tools

import com.latenighthack.ktbuf.net.ServerDescriptor
import com.latenighthack.ktbuf.net.ServerMethod
import com.latenighthack.ktbuf.net.ServerMethodDescriptor
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
import java.util.concurrent.TimeUnit

internal fun rpcResultOutcome(result: com.latenighthack.ktbuf.proto.Enum?): String {
    if (result == null) return "completed"
    // Protocols differ: lockers OK=0, while social JOIN_RESULT_OK=1 and
    // UNSPECIFIED=0. Classify generated enum symbols, never numeric ordinals.
    val name = result.javaClass.simpleName
    return if (name in setOf("OK", "SUCCESS") || listOf("_OK", "_SUCCESS").any(name::endsWith)) "success" else "error"
}

/** Wrap compiled handlers, so HTTP 200 does not hide an application failure.
 * These are attempt metrics; a redirect/retry is never a new logical journey.
 */
@Suppress("UNCHECKED_CAST")
fun ServerDescriptor.measured(registry: MeterRegistry): ServerDescriptor = copy(
    methods =
    methods.map { raw ->
        val method = raw as ServerMethodDescriptor<Any, Any, Any>
        val handler = method.handler
        if (handler !is ServerMethod.Unary) {
            method
        } else {
            val operation = "$packageName.$serviceName/${method.methodName}"
            method.copy(
                handler =
                ServerMethod.Unary { context, request ->
                    val started = System.nanoTime()
                    var outcome = "error"
                    val active =
                        runCatching {
                            io.micrometer.core.instrument.LongTaskTimer
                                .builder("fullhouse.rpc.active")
                                .tag("operation", operation)
                                .register(registry)
                                .start()
                        }.getOrNull()
                    try {
                        val response = handler.handler.invoke(this, context, request)
                        // Telemetry must never change a successful response into a failure.
                        val result =
                            runCatching {
                                val getter =
                                    response.javaClass.methods.firstOrNull {
                                        it.name == "getResult" &&
                                            it.parameterCount == 0
                                    }
                                getter?.invoke(response) as? com.latenighthack.ktbuf.proto.Enum
                            }.getOrNull()
                        outcome = rpcResultOutcome(result)
                        response
                    } catch (
                        error: TimeoutCancellationException,
                    ) {
                        outcome = "timeout"
                        throw error
                    } catch (error: CancellationException) {
                        outcome = "cancelled"
                        throw error
                    } finally {
                        registry.safeMeters {
                            active?.stop()
                            timer("fullhouse.rpc.duration", "operation", operation, "outcome", outcome)
                                .record(System.nanoTime() - started, TimeUnit.NANOSECONDS)
                        }
                    }
                },
            )
        }
    },
)
