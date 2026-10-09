package com.latenighthack.lockers.observability

import kotlinx.coroutines.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.time.TimeSource

/** Stable, bounded operation vocabulary. Never derive these names from application data. */
@Serializable
enum class TelemetryOperation(val component: String, val operation: String) {
    ROOM_CAPABILITIES("room", "capabilities"), ROOM_GET("room", "get"), ROOM_GET_ALL("room", "getall"),
    ROOM_GET_MANY("room", "getmany"), ROOM_SNAPSHOT("room", "snapshot"), ROOM_WRITE("room", "write"),
    ROOM_BATCH_WRITE("room", "batch_write"), ROOM_DELETE("room", "delete"), ROOM_LOCK("room", "lock"),
    ROOM_UNLOCK("room", "unlock"), ROOM_SUBSCRIPTION("room", "subscription"),
    SESSION_CREATE("session", "create"), SESSION_OPEN("session", "open"), SESSION_DESTROY("session", "destroy"),
    SESSION_POST("session", "post"), SESSION_POST_MANY("session", "postmany"), SESSION_BROADCAST("session", "broadcast"),
    PUSH_REGISTER("push", "register"), PUSH_UNREGISTER("push", "unregister"), PUSH_CONFIG("push", "config"), PUSH_ENQUEUE("push", "enqueue"),
    CONNECTOR_OPEN("connector", "open"), CONNECTOR_RECONNECT("connector", "reconnect"), CONNECTOR_TERMINAL("connector", "terminal"),
    CONNECTOR_SUBSCRIBE("connector", "subscribe"), CONNECTOR_ACK("connector", "ack"), CONNECTOR_GET("connector", "get"),
    CONNECTOR_GET_ALL("connector", "getall"), CONNECTOR_WRITE("connector", "write"), CONNECTOR_BATCH_WRITE("connector", "batch_write"),
    CONNECTOR_DELETE("connector", "delete"), CONNECTOR_CONFLICT("connector", "conflict"), CONNECTOR_QUEUE("connector", "queue_wait"),
    CONNECTOR_PUSH_REGISTER("connector", "push_register"), CONNECTOR_PUSH_UNREGISTER("connector", "push_unregister"),
    CONNECTOR_CODEC_ENCODE("connector", "codec_encode"), CONNECTOR_CODEC_DECODE("connector", "codec_decode"),
    STORAGE_READ("storage", "read"), STORAGE_WRITE("storage", "write"), STORAGE_TRANSACTION("storage", "transaction"),
    WRITE_QUEUE("write", "room_queue"), WRITE_OWNERSHIP("write", "ownership"), WRITE_FORWARD("write", "forward"),
    WRITE_SOURCE("write", "source_validation_commit"), WRITE_DERIVED("write", "derived_commit"),
    AGENT_EXECUTE("agent", "execute"), DELIVERY_DRAIN("delivery", "drain"), DELIVERY_GATEWAY("delivery", "gateway"),
    OWNERSHIP_RENEW("ownership", "renew"), PUSH_SEND("push", "send");
}
@Serializable enum class TelemetryOutcome { OK, REJECTED, CONFLICT, REDIRECT, ERROR, CANCELLED, DROPPED }
@Serializable enum class ClientPlatform { JVM, ANDROID, IOS, JS }
@Serializable enum class TelemetryLoss { OVERFLOW, UPLOAD, OVERSIZE }

/** Propagate tracing into client-owned shared work without adopting the caller's Job. */
class TelemetryContext(val tracing: CoroutineContext, val traceId: String? = null, val spanId: String? = null) : AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<TelemetryContext>
}

interface TelemetrySpan {
    val context: CoroutineContext get() = EmptyCoroutineContext
    val traceId: String? get() = null
    val spanId: String? get() = null
    fun finish(outcome: TelemetryOutcome)
}
fun interface TelemetryTraceBridge { suspend fun start(operation: TelemetryOperation): TelemetrySpan? }

/** Observers may fail without failing a library operation. The parent owns exporter lifecycle. */
interface LockersTelemetry {
    suspend fun startSpan(operation: TelemetryOperation): TelemetrySpan? = null
    fun record(operation: TelemetryOperation, outcome: TelemetryOutcome, elapsedNanos: Long) {}
    fun event(event: TelemetryEvent) {}
    companion object { val NONE: LockersTelemetry = object : LockersTelemetry {} }
}
fun LockersTelemetry.safeRecord(operation: TelemetryOperation, outcome: TelemetryOutcome, elapsedNanos: Long = 0) { try { record(operation, outcome, elapsedNanos) } catch (_: Exception) {} }
fun LockersTelemetry.safeEvent(event: TelemetryEvent) { try { event(event) } catch (_: Exception) {} }

suspend fun <T> LockersTelemetry.observe(
    operation: TelemetryOperation,
    outcome: (T) -> TelemetryOutcome = { TelemetryOutcome.OK },
    block: suspend () -> T,
): T {
    if (this === LockersTelemetry.NONE) return block()
    val began = TimeSource.Monotonic.markNow()
    val span = try { startSpan(operation) } catch (cancelled: CancellationException) { throw cancelled } catch (_: Exception) { null }
    var result = TelemetryOutcome.ERROR
    try {
        return (if (span == null) block() else withContext(span.context.minusKey(Job) + TelemetryContext(span.context.minusKey(Job), span.traceId, span.spanId)) { block() }).also { result = outcome(it) }
    } catch (cancelled: CancellationException) { result = TelemetryOutcome.CANCELLED; throw cancelled }
    finally {
        try { record(operation, result, began.elapsedNow().inWholeNanoseconds) } catch (_: Exception) {}
        try { span?.finish(result) } catch (_: Exception) {}
        val lifecycle = operation in setOf(TelemetryOperation.SESSION_CREATE, TelemetryOperation.SESSION_OPEN,
            TelemetryOperation.SESSION_DESTROY, TelemetryOperation.CONNECTOR_SUBSCRIBE,
            TelemetryOperation.CONNECTOR_PUSH_REGISTER, TelemetryOperation.CONNECTOR_PUSH_UNREGISTER)
        if (result != TelemetryOutcome.CANCELLED && (result != TelemetryOutcome.OK || lifecycle))
            safeEvent(TelemetryEvent(operation, result, span?.traceId, span?.spanId))
    }
}

@Serializable data class TelemetryEvent(val operation: TelemetryOperation, val outcome: TelemetryOutcome, val traceId: String? = null, val spanId: String? = null)
/** Bucket counts are disjoint; the final bucket is +Inf. Server exposition is cumulative. */
@Serializable data class ClientMeasurement(val operation: TelemetryOperation, val outcome: TelemetryOutcome, val count: Long, val sumSeconds: Double, val buckets: List<Long>)
@Serializable data class ClientLoss(val reason: TelemetryLoss, val count: Long)
@Serializable data class ClientDiagnostic(val event: TelemetryEvent? = null, val span: ClientTelemetrySpan? = null)
@Serializable data class ClientTelemetrySpan(val operation: TelemetryOperation, val outcome: TelemetryOutcome, val traceId: String, val spanId: String, val parentSpanId: String? = null, val startTimeUnixNano: Long, val endTimeUnixNano: Long)
@Serializable data class ClientTelemetryBatch(val schemaVersion: Int = 1, val platform: ClientPlatform, val measurements: List<ClientMeasurement> = emptyList(), val diagnostics: List<ClientDiagnostic> = emptyList(), val losses: List<ClientLoss> = emptyList())
fun interface ClientTelemetryTransport { suspend fun send(batch: ClientTelemetryBatch) }

object TelemetryContract {
    const val VERSION = 1
    const val MAX_BYTES = 65_536
    const val MAX_DIAGNOSTICS = 32
    val latencyBucketsSeconds = listOf(.001, .005, .01, .025, .05, .1, .25, .5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0)
    val json = Json { encodeDefaults = true; ignoreUnknownKeys = false }
    fun encode(batch: ClientTelemetryBatch): String = json.encodeToString(ClientTelemetryBatch.serializer(), batch)
    fun decode(body: String): ClientTelemetryBatch {
        require(body.encodeToByteArray().size <= MAX_BYTES)
        return json.decodeFromString(ClientTelemetryBatch.serializer(), body).also(::validate)
    }
    fun validate(batch: ClientTelemetryBatch) {
        require(batch.schemaVersion == VERSION)
        require(batch.measurements.size <= TelemetryOperation.entries.size * TelemetryOutcome.entries.size)
        require(batch.diagnostics.size <= MAX_DIAGNOSTICS && batch.losses.size <= TelemetryLoss.entries.size)
        require(batch.measurements.map { it.operation to it.outcome }.distinct().size == batch.measurements.size)
        require(batch.losses.map { it.reason }.distinct().size == batch.losses.size)
        batch.measurements.forEach {
            require(it.operation.component == "connector")
            require(it.count in 0..1_000_000 && it.sumSeconds.isFinite() && it.sumSeconds >= 0 && it.sumSeconds <= it.count * 86_400.0)
            require(it.buckets.size == latencyBucketsSeconds.size + 1 && it.buckets.all { n -> n in 0..1_000_000 } && it.buckets.sum() == it.count)
        }
        batch.losses.forEach { require(it.count in 0..1_000_000) }
        batch.diagnostics.forEach { diagnostic ->
            require((diagnostic.event == null) != (diagnostic.span == null))
            diagnostic.event?.let { require(it.operation.component == "connector"); validateIds(it.traceId, it.spanId) }
            diagnostic.span?.let { require(it.operation.component == "connector"); validateIds(it.traceId, it.spanId); it.parentSpanId?.let { id -> require(validId(id, 16)) }; require(it.startTimeUnixNano > 0 && it.endTimeUnixNano >= it.startTimeUnixNano) }
        }
    }
    private fun validateIds(trace: String?, span: String?) { require((trace == null) == (span == null)); if (trace != null) require(validId(trace, 32) && validId(span!!, 16)) }
    private fun validId(id: String, length: Int) = id.length == length && id.all { it in '0'..'9' || it in 'a'..'f' } && id.any { it != '0' }
}
