package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.ktstore.LocalContinuation
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.server.storage.v2.ServerAgentWork
import com.latenighthack.lockers.server.tools.ServiceLifecycle
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext

/**
 * In-process change notices for individual agent work items. A notice is sent only after the
 * transaction that changed the item has returned, so a woken observer reads committed state.
 * Notices are hints: observers keep a bounded fallback read because another node may execute.
 */
internal class AgentWorkSignals {
    private class Watch { val changes = MutableStateFlow(0L); var watchers = 0 }
    private val watches = ConcurrentHashMap<String, Watch>()
    private fun key(room: ByteArray, id: ByteArray) = room.joinToString("") { "%02x".format(it) } + ":" +
        id.joinToString("") { "%02x".format(it) }
    fun changed(room: ByteArray, id: ByteArray) {
        val watch = watches[key(room, id)] ?: return
        while (true) {
            val current = watch.changes.value
            if (watch.changes.compareAndSet(current, current + 1)) return
        }
    }
    /** Registers before [block] runs so a change that commits during [block] is never missed. */
    suspend fun <T> watch(room: ByteArray, id: ByteArray, block: suspend (StateFlow<Long>) -> T): T {
        val key = key(room, id)
        val watch = requireNotNull(watches.compute(key) { _, old -> (old ?: Watch()).also { it.watchers++ } })
        try { return block(watch.changes) }
        finally { watches.compute(key) { _, old -> old?.also { it.watchers-- }?.takeIf { it.watchers > 0 } } }
    }
}

/**
 * Fixed owned recovery workers; source request cancellation cannot cancel accepted agent work.
 *
 * Discovery is woken in-process when a commit enqueues agent work or an execution finishes. A slow
 * safety-net scan (backing off from [idleScanMinMs] to [idleScanMaxMs]) still finds work committed by
 * another node, recovered after a crash, or delayed by a retry, so no work depends on a notice.
 */
@Suppress("LongParameterList")
internal class AgentWorkflow(private val outbox: DeliveryOutboxStore, private val ownership: RoomOwnership,
    private val version: String, private val execute: suspend (ServerAgentWork, RoomMutationFence) -> Unit,
    parent: CoroutineContext, private val idleScanMinMs: Long = IDLE_SCAN_MIN_MS,
    private val idleScanMaxMs: Long = IDLE_SCAN_MAX_MS,
    private val workers: Int = WORKERS, private val recountCapacityAfterMs: Long = RECOUNT_CAPACITY_AFTER_MS,
) {
    private companion object {
        const val IDLE_SCAN_MIN_MS = 1_000L
        const val IDLE_SCAN_MAX_MS = 5_000L
        const val WORKERS = 4
        const val RECOUNT_CAPACITY_AFTER_MS = 10 * 60_000L
        const val FAILURE_RETRY_MS = 250L
    }
    init { require(idleScanMinMs in 1..idleScanMaxMs && workers > 0) }
    private val scope = CoroutineScope(parent + Dispatchers.IO + SupervisorJob(parent[Job]) + ServiceLifecycle.context)
    private val logger = LoggerFactory.getLogger(AgentWorkflow::class.java)
    private val started = java.util.concurrent.atomic.AtomicBoolean(false)
    private val wakeups = Channel<Unit>(Channel.CONFLATED)
    private val dispatched = Channel<ByteArray>(Channel.UNLIMITED)
    // At most one item per room is in flight; the claim only admits a room's first active item.
    private val busyRooms = ConcurrentHashMap.newKeySet<List<Byte>>()
    val signals = AgentWorkSignals()

    /** Requests a discovery scan after a commit that enqueued (or unblocked) agent work. */
    fun wake() { wakeups.trySend(Unit) }

    /**
     * Runs [room]'s first due item without a discovery scan, after a local commit enqueued it. A
     * room already in flight is left to that worker, whose completion wakes discovery.
     */
    fun submit(room: ByteArray) {
        if (busyRooms.add(room.toList())) dispatched.trySend(room.copyOf())
    }

    fun start() {
        check(started.compareAndSet(false, true))
        scope.launch { discover() }
        repeat(workers) { scope.launch { for (room in dispatched) run(room) } }
        scope.launch {
            // Counters written by this version are exact; recount once after a rolling deploy's
            // previous version, which admitted rows without counting them, has stopped.
            delay(recountCapacityAfterMs)
            try { outbox.recountCapacity() }
            catch (cancelled: CancellationException) { throw cancelled }
            catch (error: Exception) {
                logger.warn("Capacity counter recount failed; counters remain authoritative", error)
            }
        }
        scope.launch {
            while (isActive) {
                try { outbox.pruneAgentReceipts() }
                catch (cancelled: CancellationException) { throw cancelled }
                catch (error: Exception) { logger.warn("Agent receipt retention maintenance failed", error) }
                delay(60_000)
            }
        }
    }

    private suspend fun discover() {
        while (currentCoroutineContext().isActive) {
            try { outbox.initializeAgentReceipts(); break }
            catch (cancelled: CancellationException) { throw cancelled }
            catch (error: Exception) {
                logger.warn("Agent receipt adoption failed; retrying", error); delay(FAILURE_RETRY_MS)
            }
        }
        var idle = idleScanMinMs
        while (currentCoroutineContext().isActive) {
            val wait = try {
                // Dispatching alone does not reset the backoff: a room blocked behind indeterminate
                // work stays due without running. Executions wake discovery themselves.
                val nextDue = scan()
                val untilDue = nextDue?.let { (it - System.currentTimeMillis()).coerceAtLeast(1) } ?: Long.MAX_VALUE
                minOf(idle, untilDue)
            } catch (cancelled: CancellationException) { throw cancelled }
            catch (error: Exception) { logger.warn("Agent work discovery failed; retrying", error); FAILURE_RETRY_MS }
            val woken = withTimeoutOrNull(wait) { wakeups.receive() } != null
            idle = if (woken) idleScanMinMs else minOf(idle * 2, idleScanMaxMs)
        }
    }

    /** Dispatches due work to idle rooms; returns the next future due time within the scan horizon. */
    private suspend fun scan(): Long? {
        val now = System.currentTimeMillis()
        var after: LocalContinuation? = null
        do {
            val page = outbox.agentWork.upcoming(now, now + idleScanMaxMs, after)
            for (candidate in page.due) {
                if (busyRooms.add(candidate.roomId.toList())) dispatched.send(candidate.roomId)
            }
            if (page.nextDueAt != null) return page.nextDueAt
            after = page.continuation
        } while (after != null)
        return null
    }

    private suspend fun run(roomId: ByteArray) {
        var executed = false
        try {
            val room = RoomId(roomId)
            val fence = try { ownership.mutationFence(0, room) } catch (_: RoomOwnershipLost) { return }
            // Dispatch only names a room with due work; the room's first active item runs.
            val claimed = outbox.agentWork.claimNext(room, version, System.currentTimeMillis()) ?: return
            executed = true
            try { execute(claimed, fence) }
            catch (cancelled: CancellationException) { if (!currentCoroutineContext().isActive) throw cancelled }
            catch (error: Exception) { logger.warn("Agent work remains recoverable", error) }
            finally { signals.changed(claimed.roomId, claimed.writeRequestId) }
        } catch (cancelled: CancellationException) { throw cancelled }
        catch (error: Exception) { logger.warn("Agent work claim failed; work remains recoverable", error) }
        finally {
            busyRooms.remove(roomId.toList())
            // The room's next item, or a retry, may now be due.
            if (executed) wake()
        }
    }

    suspend fun closeAndJoin() = withContext(NonCancellable) { scope.coroutineContext[Job]!!.cancelAndJoin() }
}
