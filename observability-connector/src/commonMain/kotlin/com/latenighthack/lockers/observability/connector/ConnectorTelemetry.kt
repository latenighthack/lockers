package com.latenighthack.lockers.observability.connector

import com.latenighthack.lockers.observability.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/** Bounded, at-most-once telemetry. Transport must not retry or instrument its own requests. */
class ConnectorTelemetry(
    private val platform: ClientPlatform = clientPlatform(),
    private val transport: ClientTelemetryTransport,
    private val traceBridge: TelemetryTraceBridge = TelemetryTraceBridge { null },
    private val metricsIntervalMillis: Long = 30_000,
    private val diagnosticsIntervalMillis: Long = 1_000,
    scope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.Default),
) : LockersTelemetry {
    private val job = SupervisorJob(scope.coroutineContext[Job])
    private val workerScope = CoroutineScope(scope.coroutineContext + job)
    private val diagnostics = Channel<ClientDiagnostic>(256)
    private val cells = MutableStateFlow<Map<Pair<TelemetryOperation, TelemetryOutcome>, ClientMeasurement>>(emptyMap())
    private val losses = MutableStateFlow(TelemetryLoss.entries.associateWith { 0L })
    private val flushLock = Mutex()
    private val closed = MutableStateFlow(false)
    private val metricsWorker = workerScope.launch { while (isActive) { delay(metricsIntervalMillis); flushMetrics() } }
    private val diagnosticsWorker = workerScope.launch { while (isActive) { delay(diagnosticsIntervalMillis); flushDiagnostics() } }
    private fun lost(reason: TelemetryLoss, count: Long = 1) { losses.update { it + (reason to (it.getValue(reason) + count).coerceAtMost(1_000_000)) } }
    override suspend fun startSpan(operation: TelemetryOperation) = traceBridge.start(operation)
    override fun record(operation: TelemetryOperation, outcome: TelemetryOutcome, elapsedNanos: Long) {
        if (closed.value || operation.component != "connector") return
        val seconds = elapsedNanos.coerceAtLeast(0).coerceAtMost(86_400_000_000_000L) / 1_000_000_000.0
        val bucket = TelemetryContract.latencyBucketsSeconds.indexOfFirst { seconds <= it }.let { if (it < 0) TelemetryContract.latencyBucketsSeconds.size else it }
        val previous = cells.getAndUpdate { state ->
            val key = operation to outcome
            val cell = state[key] ?: ClientMeasurement(operation, outcome, 0, 0.0, List(TelemetryContract.latencyBucketsSeconds.size + 1) { 0 })
            if (cell.count >= 1_000_000) state else state + (key to cell.copy(count = cell.count + 1, sumSeconds = cell.sumSeconds + seconds, buckets = cell.buckets.mapIndexed { i, n -> if (i == bucket) n + 1 else n }))
        }
        if ((previous[operation to outcome]?.count ?: 0) >= 1_000_000) lost(TelemetryLoss.OVERFLOW)
    }
    override fun event(event: TelemetryEvent) { if (!closed.value && diagnostics.trySend(ClientDiagnostic(event = event)).isFailure) lost(TelemetryLoss.OVERFLOW) }
    fun offer(span: ClientTelemetrySpan) { if (!closed.value && diagnostics.trySend(ClientDiagnostic(span = span)).isFailure) lost(TelemetryLoss.OVERFLOW) }
    suspend fun flushMetrics() = flushLock.withLock {
        val drained = cells.getAndUpdate { emptyMap() }.values.toList()
        val lost = losses.getAndUpdate { TelemetryLoss.entries.associateWith { 0L } }.filterValues { it > 0 }.map { ClientLoss(it.key, it.value) }
        if (drained.isNotEmpty() || lost.isNotEmpty()) send(ClientTelemetryBatch(platform = platform, measurements = drained, losses = lost))
    }
    suspend fun flushDiagnostics() = flushLock.withLock {
        val batch = mutableListOf<ClientDiagnostic>()
        repeat(TelemetryContract.MAX_DIAGNOSTICS) { diagnostics.tryReceive().getOrNull()?.let { batch += it } }
        if (batch.isNotEmpty()) { send(ClientTelemetryBatch(platform = platform, diagnostics = batch)); true } else false
    }
    private suspend fun send(batch: ClientTelemetryBatch) {
        fun retainLosses() { batch.losses.forEach { lost(it.reason, it.count) } }
        if (TelemetryContract.encode(batch).encodeToByteArray().size > TelemetryContract.MAX_BYTES) { retainLosses(); lost(TelemetryLoss.OVERSIZE); return }
        try { withTimeout(5_000) { transport.send(batch) } }
        catch (_: TimeoutCancellationException) { retainLosses(); lost(TelemetryLoss.UPLOAD) }
        catch (cancelled: CancellationException) { retainLosses(); lost(TelemetryLoss.UPLOAD); throw cancelled }
        catch (_: Exception) { retainLosses(); lost(TelemetryLoss.UPLOAD) }
    }
    fun close() {
        if (!closed.compareAndSet(false, true)) return
        diagnostics.close()
        metricsWorker.cancel(); diagnosticsWorker.cancel()
        workerScope.launch { try { withTimeout(2_000) { flushMetrics(); while (flushDiagnostics()) yield() } } finally { job.cancel() } }
    }
    suspend fun awaitClosed() { job.join() }
}
