package com.latenighthack.lockers.server.tools

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanContext
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.TraceFlags
import io.opentelemetry.api.trace.TraceState
import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import kotlinx.coroutines.withContext

private const val MAX_PRODUCER_LINKS = 64
private const val TRACE_ID_START = 3
private const val TRACE_ID_END = 35
private const val SPAN_ID_START = 36
private const val SPAN_ID_END = 52
private const val TRACE_FLAGS_START = 53

/** W3C context is operational metadata, never part of signed application data. */
fun producerTraceparent(): String = runCatching {
    val value = Span.current().spanContext
    if (value.isValid) "00-${value.traceId}-${value.spanId}-${if (value.isSampled) "01" else "00"}" else ""
}.getOrDefault("")

// Record diagnostics and rethrow the original failure, including cancellation.
@Suppress("TooGenericExceptionCaught")
suspend fun <T> traceWork(
    operation: String,
    producers: List<String>,
    telemetry: OpenTelemetry = GlobalOpenTelemetry.get(),
    block: suspend () -> T,
): T {
    val span =
        runCatching {
            val contexts =
                producers.distinct().take(MAX_PRODUCER_LINKS).mapNotNull { parent ->
                    if (!parent.matches(Regex("00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]"))) {
                        null
                    } else {
                        SpanContext
                            .createFromRemoteParent(
                                parent.substring(TRACE_ID_START, TRACE_ID_END),
                                parent.substring(SPAN_ID_START, SPAN_ID_END),
                                TraceFlags.fromHex(parent, TRACE_FLAGS_START),
                                TraceState.getDefault(),
                            ).takeIf { it.isValid }
                    }
                }
            val builder = telemetry.getTracer("lockers.work").spanBuilder(operation).setSpanKind(SpanKind.CONSUMER)
            if (contexts.size == 1) {
                builder.setParent(Context.root().with(Span.wrap(contexts.single())))
            } else {
                builder.setNoParent()
            }
            contexts.forEach { builder.addLink(it) }
            builder.startSpan()
        }.getOrNull() ?: return block()
    try {
        return withContext(Context.current().with(span).asContextElement()) { block() }
    } catch (
        failure: Throwable,
    ) {
        runCatching {
            span.setStatus(StatusCode.ERROR)
            span.setAttribute("error.type", failure.javaClass.simpleName)
        }
        throw failure
    } finally {
        runCatching { span.end() }
    }
}
