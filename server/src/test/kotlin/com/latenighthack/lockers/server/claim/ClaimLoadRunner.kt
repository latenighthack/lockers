package com.latenighthack.lockers.server.claim

import com.latenighthack.ktbuf.rpc.HttpRpcClient
import com.latenighthack.lockers.common.v1.Locker
import com.latenighthack.lockers.common.v1.LockerId
import com.latenighthack.lockers.common.v1.LockerKeyspace
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.room.v1.PostLockerChangeRequest
import com.latenighthack.lockers.room.v1.PostLockerChangeResponse
import com.latenighthack.lockers.room.v1.RoomServiceRpc
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration

/**
 * Write-load generator for claim-mode clusters (the design doc's load plan). Models a smart
 * client: each room's first write goes to an arbitrary node; on `NOT_OWNER` the redirect is
 * followed once and the owner cached — exactly the connector's behavior. Every write targets a
 * fresh locker id so the locker-version CAS never rejects (this measures the ownership + write
 * path, not CAS retries).
 */
class ClaimLoadRunner(
    private val targetAddrs: List<String>,
    private val rooms: Int,
    private val writesPerSecond: Int,
    private val duration: Duration,
    // Enough writers that each one stays comfortably under ~30 writes/s (latency headroom).
    private val writers: Int = (writesPerSecond / 25).coerceIn(MIN_WRITERS, MAX_WRITERS),
) {
    data class Result(
        val writes: Int,
        val errors: Int,
        val redirectsFollowed: Int,
        val p50Ms: Double,
        val p99Ms: Double,
        val maxMs: Double,
        val errorBreakdown: Map<String, Int>,
    ) {
        fun report(label: String): String = buildString {
            appendLine("== $label ==")
            appendLine("writes=$writes errors=$errors redirectsFollowed=$redirectsFollowed")
            appendLine(
                "latency p50=%.2fms p99=%.2fms max=%.2fms".format(p50Ms, p99Ms, maxMs)
            )
            if (errorBreakdown.isNotEmpty()) appendLine("errors by kind: $errorBreakdown")
        }
    }

    // Schemeless on purpose: HttpRpcClient prepends "http://" itself for non-https paths.
    private val clients = targetAddrs.associateWith { RoomServiceRpc(HttpRpcClient(it)) }
    private val ownerCache = ConcurrentHashMap<Int, String>()
    private val latenciesNanos = ConcurrentLinkedQueue<Long>()
    private val errorKinds = ConcurrentHashMap<String, AtomicInteger>()
    private val errors = AtomicInteger(0)
    private val redirectsFollowed = AtomicInteger(0)
    private val lockerCounter = AtomicLong(0)

    suspend fun run(): Result {
        val perWriterIntervalMs = (1000L * writers / writesPerSecond).coerceAtLeast(1)
        val deadlineNanos = System.nanoTime() + duration.inWholeNanoseconds
        coroutineScope {
            repeat(writers) { w ->
                launch {
                    var i = w
                    while (System.nanoTime() < deadlineNanos) {
                        writeOnce(i % rooms)
                        i += writers
                        delay(perWriterIntervalMs)
                    }
                }
            }
        }
        val sorted = latenciesNanos.toLongArray().also { it.sort() }
        fun pct(p: Double): Double =
            if (sorted.isEmpty()) 0.0
            else sorted[((sorted.size - 1) * p).toInt()] / 1_000_000.0
        return Result(
            writes = sorted.size,
            errors = errors.get(),
            redirectsFollowed = redirectsFollowed.get(),
            p50Ms = pct(0.50),
            p99Ms = pct(0.99),
            maxMs = if (sorted.isEmpty()) 0.0 else sorted.last() / 1_000_000.0,
            errorBreakdown = errorKinds.mapValues { it.value.get() },
        )
    }

    // Locker ids carry a per-run token + counter: unique within the run AND across runs, so stale
    // locker rows from a previous run against the same database never trip the version CAS.
    private val runToken = java.util.UUID.randomUUID().mostSignificantBits

    private fun post(room: Int) = PostLockerChangeRequest {
        roomId = RoomId("load-room-$room".encodeToByteArray())
        lockerId = LockerId {
            rawValue = ByteBuffer.allocate(2 * Long.SIZE_BYTES)
                .putLong(runToken)
                .putLong(lockerCounter.incrementAndGet())
                .array()
            keyspace = LockerKeyspace { value = 1 }
        }
        locker = Locker { open { encodedPayload = PAYLOAD } }
    }

    private fun countError(kind: String) {
        errors.incrementAndGet()
        errorKinds.computeIfAbsent(kind) { AtomicInteger(0) }.incrementAndGet()
    }

    private suspend fun writeOnce(room: Int) {
        val preferred = ownerCache[room] ?: targetAddrs[room % targetAddrs.size]
        val start = System.nanoTime()
        try {
            var response = clients.getValue(preferred).postLockerChange(post(room))
            if (response.result is PostLockerChangeResponse.Result.NOT_OWNER) {
                val ownerAddr = response.redirect?.ownerAddress
                val ownerClient = ownerAddr?.let { clients[it] }
                if (ownerClient == null) {
                    countError("redirect-to-unknown:$ownerAddr")
                    return
                }
                redirectsFollowed.incrementAndGet()
                ownerCache[room] = ownerAddr
                response = ownerClient.postLockerChange(post(room))
            }
            latenciesNanos.add(System.nanoTime() - start)
            if (response.result !is PostLockerChangeResponse.Result.OK) {
                countError("result:${response.result?.let { it::class.simpleName } ?: "null"}")
            }
        } catch (t: Exception) {
            countError("exception:${t::class.simpleName}:${t.message?.take(80)}")
        }
    }

    companion object {
        private const val MIN_WRITERS = 16
        private const val MAX_WRITERS = 96
        private val PAYLOAD = ByteArray(64) { it.toByte() }
    }
}

/** Env-tunable load knobs shared by the in-process soak test and the external driver. */
object LoadKnobs {
    fun nodes() = System.getenv("LOCKERS_LOAD_NODES")?.toIntOrNull() ?: 2
    fun rooms() = System.getenv("LOCKERS_LOAD_ROOMS")?.toIntOrNull() ?: 1_000
    fun writesPerSecond() = System.getenv("LOCKERS_LOAD_WPS")?.toIntOrNull() ?: 50
    fun durationSeconds() = System.getenv("LOCKERS_LOAD_DURATION_SEC")?.toLongOrNull() ?: 30L
}
