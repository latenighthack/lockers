package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.ktstore.LocalContinuation
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.server.storage.v2.ServerAgentWork
import com.latenighthack.lockers.server.tools.ServiceLifecycle
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import kotlin.coroutines.CoroutineContext

/** Fixed owned recovery workers; source request cancellation cannot cancel accepted agent work. */
internal class AgentWorkflow(private val outbox: DeliveryOutboxStore, private val ownership: RoomOwnership,
    private val version: String, private val execute: suspend (ServerAgentWork, RoomMutationFence) -> Unit,
    parent: CoroutineContext,
) {
    private val scope = CoroutineScope(parent + Dispatchers.IO + SupervisorJob(parent[Job]) + ServiceLifecycle.context)
    private val logger = LoggerFactory.getLogger(AgentWorkflow::class.java)
    private val started = java.util.concurrent.atomic.AtomicBoolean(false)
    fun start() {
        check(started.compareAndSet(false, true))
        repeat(4) {
            scope.launch {
                var after: LocalContinuation? = null
                var scanTime = System.currentTimeMillis()
                while (isActive) {
                    try { outbox.initializeAgentReceipts(); break }
                    catch (cancelled: CancellationException) { throw cancelled }
                    catch (error: Exception) { logger.warn("Agent receipt adoption failed; retrying", error); delay(250) }
                }
                while (isActive) {
                    try {
                        val page = outbox.agentWork.candidates(scanTime, after)
                        after = page.second
                        for (candidate in page.first) {
                            ensureActive()
                            val room = RoomId(candidate.roomId)
                            val fence = try { ownership.mutationFence(0, room) } catch (_: RoomOwnershipLost) { continue }
                            val claimed = outbox.agentWork.claim(room, candidate.writeRequestId, version, System.currentTimeMillis()) ?: continue
                            try { execute(claimed, fence) }
                            catch (cancelled: CancellationException) { if (!currentCoroutineContext().isActive) throw cancelled }
                            catch (error: Exception) { logger.warn("Agent work remains recoverable", error) }
                        }
                        if (after == null) { scanTime = System.currentTimeMillis(); delay(50) }
                    } catch (cancelled: CancellationException) { throw cancelled }
                    catch (error: Exception) { logger.warn("Agent work discovery failed; retrying", error); after = null; scanTime = System.currentTimeMillis(); delay(250) }
                }
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
    suspend fun closeAndJoin() = withContext(NonCancellable) { scope.coroutineContext[Job]!!.cancelAndJoin() }
}
