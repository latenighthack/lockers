package com.latenighthack.lockers.server.tools

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.withContext
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.ConcurrentHashMap

private const val MAX_METRIC_SHARDS = 32
private const val DISPATCHER_STAGES = 3

class ShardedDispatcher<T>(
    private val shardCount: Int,
    name: String,
    private val registry: io.micrometer.core.instrument.MeterRegistry? = null,
    private val idHashCode: (T) -> Int
) {
    constructor(shardCount: Int, name: String, idHashCode: (T) -> Int) : this(shardCount, name, null, idHashCode)

    @OptIn(DelicateCoroutinesApi::class)
    private val dispatcher = newFixedThreadPoolContext(shardCount, name)
    private val shardDispatchers: Array<CoroutineDispatcher> = Array(shardCount) {
        dispatcher.limitedParallelism(1)
    }

    private val queue = if (name.startsWith("room")) "room" else if (name.startsWith("session")) "session" else "other"
    private val states = Array(minOf(shardCount, MAX_METRIC_SHARDS)) { index ->
        Array(DISPATCHER_STAGES) { state -> java.util.concurrent.atomic.AtomicInteger().also { value ->
            registry?.safeMeters { gauge("fullhouse.dispatcher.work",
                io.micrometer.core.instrument.Tags.of("queue", queue, "shard", index.toString(), "state",
                    listOf("dispatch_wait", "mutex_wait", "active")[state]), value) { it.get().toDouble() } }
        } }
    }
    private fun record(stage: String, start: Long, outcome: String = "success") {
        registry?.safeMeters { timer("fullhouse.dispatcher.duration", "queue", queue, "stage", stage, "outcome",
            outcome)
            .record(System.nanoTime() - start, java.util.concurrent.TimeUnit.NANOSECONDS) }
    }

    private class Entry(val mutex: Mutex = Mutex(), var users: Int = 0)
    private val locks = ConcurrentHashMap<T, Entry>()
    private val operations = HashSet<CompletableDeferred<Unit>>()
    private val gate = Any()
    private val closing = Mutex()
    private var closed = false

    suspend fun <U> runOnDispatcher(id: T, block: suspend () -> U): U = execute(id, true, block)

    /** For effects already serialized by a storage transaction: never wait on a key mutex while owning that transaction. */
    internal suspend fun <U> runWithoutKeyLock(id: T, block: suspend () -> U): U = execute(id, false, block)

    private suspend fun <U> execute(id: T, serialize: Boolean, block: suspend () -> U): U {
        val shardIndex = Math.floorMod(idHashCode(id), shardCount)
        val dispatcher = shardDispatchers[shardIndex]
        val ticket = CompletableDeferred<Unit>()
        synchronized(gate) { check(!closed) { "Sharded dispatcher is closed" }; operations.add(ticket) }
        val state = states[shardIndex % states.size]
        val submitted = System.nanoTime()
        state[0].incrementAndGet()
        var dispatched = false
        var entry: Entry? = null
        try {
            entry = if (serialize) locks.compute(id) { _, existing -> (existing ?: Entry()).also { it.users++ } }!! else null
            val current = entry
            return withContext(dispatcher + ServiceLifecycle.context) {
                dispatched = true
                state[0].decrementAndGet()
                record("dispatch_wait", submitted)
                if (current != null) {
                    val waiting = System.nanoTime()
                    state[1].incrementAndGet()
                    var outcome = "success"
                    try { current.mutex.lock() }
                    catch (cancelled: kotlinx.coroutines.CancellationException) { outcome = "cancelled";
                        throw cancelled }
                    finally { state[1].decrementAndGet(); record("mutex_wait", waiting, outcome) }
                }
                val started = System.nanoTime()
                state[2].incrementAndGet()
                var outcome = "error"
                try { block().also { outcome = "success" } }
                catch (cancelled: kotlinx.coroutines.CancellationException) { outcome = "cancelled"; throw cancelled }
                finally { state[2].decrementAndGet(); current?.mutex?.unlock(); record("execute", started, outcome) }
            }
        } finally {
            if (!dispatched) { state[0].decrementAndGet(); record("dispatch_wait", submitted, "cancelled") }
            if (entry != null) locks.computeIfPresent(id) { _, current -> if (--current.users == 0) null else current }
            synchronized(gate) { operations.remove(ticket); ticket.complete(Unit) }
        }
    }

    fun dispatcherFor(id: T): CoroutineDispatcher = synchronized(gate) {
        check(!closed) { "Sharded dispatcher is closed" }
        shardDispatchers[Math.floorMod(idHashCode(id), shardCount)]
    }

    fun contextFor(id: T): CoroutineContext = dispatcherFor(id)

    /** Releases the backing thread pool. The dispatcher is unusable afterwards. */
    suspend fun closeAndJoin() {
        ServiceLifecycle.requireExternalClose()
        closing.withLock {
            val pending = synchronized(gate) { closed = true; operations.toList() }
            kotlinx.coroutines.withContext(kotlinx.coroutines.NonCancellable) {
                pending.forEach { it.await() }
                dispatcher.close()
            }
        }
    }
    fun close() = ServiceLifecycle.blockingClose { closeAndJoin() }
}
