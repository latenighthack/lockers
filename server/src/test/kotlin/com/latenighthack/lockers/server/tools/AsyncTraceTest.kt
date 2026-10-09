package com.latenighthack.lockers.server.tools

import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class AsyncTraceTest {
    @Test fun durableWorkRestoresProducerParentAcrossSuspensionAndRejectsMalformedContext() = runBlocking {
        val exporter = InMemorySpanExporter.create()
        val provider = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build()
        val sdk = OpenTelemetrySdk.builder().setTracerProvider(provider).build()
        try {
            val producer = sdk.getTracer("test").spanBuilder("producer").startSpan()
            val encoded = withContext(Context.current().with(producer).asContextElement()) { producerTraceparent() }
            producer.end()
            traceWork("consumer", listOf(encoded), sdk) {
                kotlinx.coroutines.yield()
                assertTrue(Span.current().spanContext.isValid)
            }
            traceWork(
                "malformed",
                listOf("00-" + "0".repeat(32) + "-" + "0".repeat(16) + "-01", "invalid"),
                sdk,
            ) { }
            val spans = exporter.finishedSpanItems.associateBy { it.name }
            assertEquals(producer.spanContext.spanId, spans.getValue("consumer").parentSpanId)
            assertEquals(producer.spanContext.traceId, spans.getValue("consumer").traceId)
            assertEquals(1, spans.getValue("consumer").links.size)
            assertTrue(spans.getValue("malformed").links.isEmpty())
            assertEquals("0".repeat(16), spans.getValue("malformed").parentSpanId)
        } finally {
            sdk.close()
            exporter.close()
        }
    }
}
