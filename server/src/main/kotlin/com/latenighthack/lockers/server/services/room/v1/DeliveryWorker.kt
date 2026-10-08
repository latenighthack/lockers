package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.server.services.session.v1.SessionGatewayDiscovery
import com.latenighthack.lockers.session.v1.*
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.slf4j.LoggerFactory
import java.util.UUID
import io.micrometer.core.instrument.MeterRegistry
import com.latenighthack.lockers.observability.*
import com.latenighthack.lockers.server.tools.safeMeters
import java.util.concurrent.TimeUnit

/** Leases recover after process death; stable event ids make retries safe at the gateway. */
class DeliveryWorker(private val store: DeliveryOutboxStore, private val discovery: SessionGatewayDiscovery, private val meters: MeterRegistry? = null, private val telemetry: LockersTelemetry = LockersTelemetry.NONE, private val queue: String = "room") {
    init { meters?.counter("lockers.delivery.accepted", "queue", queue); meters?.counter("lockers.delivery.attempts", "queue", queue); meters?.counter("lockers.delivery.failures", "queue", queue) }
    private val active = java.util.concurrent.atomic.AtomicInteger(0)
    init { meters?.gauge("lockers.delivery.worker.active", listOf(io.micrometer.core.instrument.Tag.of("queue", queue)), active) { it.get().toDouble() } }
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val permits = Semaphore(4)
    private val owner = UUID.randomUUID().toString()
    private val logger = LoggerFactory.getLogger(DeliveryWorker::class.java)
    private var job: Job? = null
    @Volatile private var stopping = false
    fun start() {
        check(job == null)
        active.set(1)
        job = scope.launch {
            // Independent room lanes keep a slow gateway from imposing a batch barrier on
            // unrelated rooms. All lanes share the same four network permits.
            coroutineScope {
                repeat(4) { launch {
                    while (isActive && !stopping) {
                        try { if (!drainOnce(roomLimit = 1)) store.awaitWork() }
                        catch (e: CancellationException) { throw e }
                        catch (e: Exception) { meters?.safeMeters { counter("lockers.delivery.failures", "queue", queue).increment() }; logger.warn("delivery outbox iteration failed", e); delay(250) }
                    }
                } }
            }
        }
    }
    suspend fun drainOnce(roomLimit: Int = 4): Boolean = telemetry.observe(TelemetryOperation.DELIVERY_DRAIN) { drainMeasured(roomLimit) }
    private suspend fun drainMeasured(roomLimit: Int): Boolean {
        val began = System.nanoTime()
        val intents = store.claim(owner, System.currentTimeMillis(), limit = roomLimit, eventsPerRoom = 64)
        if (intents.isEmpty()) return false
        meters?.safeMeters { counter("lockers.delivery.attempts", "queue", queue).increment(intents.size.toDouble()) }
        val heartbeat = CoroutineScope(kotlin.coroutines.coroutineContext).launch {
            while (isActive) { delay(10_000); store.renew(intents, System.currentTimeMillis()) }
        }
        try {
            intents.filter { it.pendingSessions.isEmpty() }.forEach { store.accepted(it, emptyList()) }
            val groups = discovery.resolveGroups(intents.flatMap { it.pendingSessions }.map { SessionId(it.rawValue) }.distinct())
            logger.debug("locker_delivery events={} recipients={} gateways={} payload_bytes={}", intents.size,
                intents.sumOf { it.pendingSessions.size }, groups.size, intents.sumOf { it.encodedEvent.size.toLong() })
            coroutineScope {
                groups.map { group -> async {
                    permits.withPermit {
                        val routed = intents.mapNotNull { intent ->
                            val pending = intent.pendingSessions.map { SessionId(it.rawValue) }.toSet()
                            val recipients = group.sessionIds.filter { it in pending }
                            if (recipients.isEmpty()) null else intent to PostEventRequest(recipients, Event.fromByteArray(intent.encodedEvent))
                        }
                        try {
                            // Each claimed room prefix is ordered. One gateway RPC can carry
                            // source and derived events without serial network round trips.
                            val batches = mutableListOf<MutableList<Pair<com.latenighthack.lockers.server.storage.v1.ServerDeliveryIntent, PostEventRequest>>>()
                            var bytes = 0
                            for (item in routed) {
                                val size = item.second.toByteArray().size + 16
                                if (batches.isEmpty() || batches.last().size == 64 || bytes + size > 8 * 1024 * 1024) {
                                    batches.add(mutableListOf()); bytes = 0
                                }
                                batches.last().add(item); bytes += size
                            }
                            for (batch in batches) {
                                val results = telemetry.observe(TelemetryOperation.DELIVERY_GATEWAY) { withTimeout(10_000) {
                                    try { group.service.postEvents(com.latenighthack.lockers.session.v1.PostEventsRequest(batch.map { it.second })).results }
                                    catch (e: com.latenighthack.ktbuf.net.RpcResponseException) {
                                        if (e.code != com.latenighthack.ktbuf.proto.Codes.UNIMPLEMENTED && e.code != com.latenighthack.ktbuf.proto.Codes.NOT_FOUND) throw e
                                        // An unsupported method cannot have committed any of its groups.
                                        batch.map { group.service.postEvent(it.second) }
                                    }
                                }
                                }
                                batch.zip(results).forEach { (item, result) ->
                                    if (result.result.isOk()) {
                                        store.accepted(item.first, item.second.sessionIds)
                                        meters?.safeMeters { counter("lockers.delivery.accepted", "queue", queue).increment() }
                                    } else meters?.safeMeters { counter("lockers.delivery.failures", "queue", queue).increment() }
                                }
                            }
                        } catch (e: CancellationException) { if (e !is TimeoutCancellationException) throw e else meters?.safeMeters { counter("lockers.delivery.failures", "queue", queue).increment() } }
                        catch (e: Exception) { meters?.safeMeters { counter("lockers.delivery.failures", "queue", queue).increment() }; logger.warn("delivery gateway retry; {} events remain recoverable", routed.size, e) }
                    }
                } }.awaitAll()
            }
        } finally {
            withContext(NonCancellable) {
                heartbeat.cancelAndJoin()
                var retried = false
                intents.forEach { if (store.retryIfPending(it, System.currentTimeMillis())) retried = true }
                if (retried) meters?.safeMeters { counter("lockers.delivery.retry.rounds", "queue", queue).increment() }
                meters?.safeMeters { timer("lockers.delivery.duration", "queue", queue).record(System.nanoTime() - began, TimeUnit.NANOSECONDS) }
            }
        }
        logger.debug("locker_delivery elapsed_ms={}", (System.nanoTime() - began) / 1_000_000.0)
        return true
    }
    /** Stops claiming. Await in-flight acceptance before disabling the last worker. */
    suspend fun stop() { stopping = true; job?.join(); active.set(0); scope.cancel() }
    suspend fun drain(timeoutMs: Long = 30_000) = withTimeout(timeoutMs) {
        stopping = true
        job?.join()
        while (store.pendingCount() > 0) { if (!drainOnce()) store.awaitWork() }
    }
    fun close() { active.set(0); scope.cancel() }
}
