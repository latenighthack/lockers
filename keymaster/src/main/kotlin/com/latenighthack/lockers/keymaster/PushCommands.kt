package com.latenighthack.lockers.keymaster

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.UsageError
import com.github.ajalt.clikt.core.requireObject
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.multiple
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import com.latenighthack.lockers.push.v1.DrainQueueRequest
import com.latenighthack.lockers.push.v1.GetQueueStatsRequest
import com.latenighthack.lockers.push.v1.ListDeadLettersRequest
import com.latenighthack.lockers.push.v1.PurgeDeadLettersRequest
import com.latenighthack.lockers.push.v1.RetryDeadLettersRequest
import kotlinx.coroutines.runBlocking

/** Strips the proto enum prefix so backends read as APNS / FCM / WEB_PUSH. */
private fun backendLabel(backend: Any?): String = backend.toString().removePrefix("PUSH_BACKEND_")

/**
 * Runs [call] against every configured node in sequence and returns the per-node results. A node
 * that errors doesn't abort the whole command — its failure is surfaced to stderr and it is skipped,
 * so an operator still gets aggregate numbers from the reachable nodes (important during a partial
 * outage, which is exactly when these commands are run). Sequential (not parallel) keeps output
 * deterministic and avoids ktbuf's OkHttp thread pile-up across many nodes.
 */
private fun <T> CliktCommand.fanOut(
    nodes: List<NodePushAdmin>,
    call: suspend (NodePushAdmin) -> T,
): List<Pair<String, T>> = runBlocking {
    nodes.mapNotNull { node ->
        try {
            node.node to call(node)
        } catch (t: Throwable) {
            echo("node ${node.node} failed: ${t.message ?: t}", err = true)
            null
        }
    }
}

/** True when more than one node is targeted (controls whether per-node breakdowns are shown). */
private fun KeymasterConfig.isClustered(): Boolean = adminNodes.size > 1

/**
 * Shared output shape for the fan-out commands that return a single count per node (drain, retry,
 * purge): a per-node breakdown [field] row when clustered, then the cluster-wide sum. Keeps the
 * three commands from copy-pasting the same sum/emit dance.
 */
private fun <T> CliktCommand.emitCountAggregate(
    config: KeymasterConfig,
    results: List<Pair<String, T>>,
    field: String,
    count: (T) -> Long,
) {
    if (config.isClustered()) {
        emitRows(config.output, results.map { (node, r) -> listOf<Pair<String, Any?>>("node" to node, field to count(r)) })
    }
    emit(config.output, listOf("nodes" to results.size, field to results.sumOf { count(it.second) }))
}

/** Parent `push` group — dispatches to its subcommands. */
class PushCommand : CliktCommand(name = "push") {
    override fun help(context: Context) = "Inspect and recover the push delivery queue (PushAdmin)."
    override fun run() = Unit
}

/**
 * `push stats` — PushAdmin.GetQueueStats, fanned across every `--node` and summed. In a cluster the
 * queue/dead-letters are sharded across nodes, so cluster-wide depth is the sum; `--per-node` also
 * prints each node's contribution.
 */
class PushStatsCommand : CliktCommand(name = "stats") {
    private val config by requireObject<KeymasterConfig>()
    private val perNode by option("--per-node", help = "Also print each node's stats separately.").flag()
    override fun help(context: Context) = "Show queue and dead-letter depths, total and per backend."
    override fun run() {
        val results = fanOut(config.pushAdmins()) { it.client.getQueueStats(GetQueueStatsRequest { }) }

        var queued = 0L
        var deadLettered = 0L
        val queuedByBackend = linkedMapOf<String, Long>()
        val deadLetteredByBackend = linkedMapOf<String, Long>()
        for ((_, stats) in results) {
            queued += stats.queued
            deadLettered += stats.deadLettered
            stats.queuedByBackend.forEach { queuedByBackend.merge(backendLabel(it.backend), it.count, Long::plus) }
            stats.deadLetteredByBackend.forEach {
                deadLetteredByBackend.merge(backendLabel(it.backend), it.count, Long::plus)
            }
        }

        if (perNode && config.isClustered()) {
            val rows = results.map { (node, stats) ->
                listOf<Pair<String, Any?>>(
                    "node" to node,
                    "queued" to stats.queued,
                    "deadLettered" to stats.deadLettered,
                )
            }
            emitRows(config.output, rows)
        }

        val fields = buildList {
            add("nodes" to results.size)
            add("queued" to queued)
            add("deadLettered" to deadLettered)
            queuedByBackend.forEach { add("queued.${it.key}" to it.value) }
            deadLetteredByBackend.forEach { add("deadLettered.${it.key}" to it.value) }
        }
        emit(config.output, fields)
    }
}

/** `push drain` — PushAdmin.DrainQueue, fanned across every `--node` (each drains its own shard). */
class PushDrainCommand : CliktCommand(name = "drain") {
    private val config by requireObject<KeymasterConfig>()
    override fun help(context: Context) = "Re-feed every persisted-but-idle queued push into the processor."
    override fun run() {
        val results = fanOut(config.pushAdmins()) { it.client.drainQueue(DrainQueueRequest { }) }
        emitCountAggregate(config, results, "drained") { it.drained }
    }
}

/** Parent `push dead-letters` group. */
class DeadLetterCommand : CliktCommand(name = "dead-letters") {
    override fun help(context: Context) = "Inspect and manage parked (dead-lettered) pushes."
    override fun run() = Unit
}

/**
 * `push dead-letters list` — PushAdmin.ListDeadLetters across every `--node`, concatenated. Each row
 * is tagged with its owning `node` so the operator knows where to target a retry/purge if needed
 * (though retry/purge fan out to all nodes anyway). `--limit` is per node.
 */
class DeadLetterListCommand : CliktCommand(name = "list") {
    private val config by requireObject<KeymasterConfig>()
    override fun help(context: Context) = "List parked pushes; copy a pushId into retry/purge."

    private val limit by option("--limit", help = "Max rows PER NODE; <= 0 uses the server default.").int().default(0)

    override fun run() {
        val clustered = config.isClustered()
        val results = fanOut(config.pushAdmins()) {
            it.client.listDeadLetters(ListDeadLettersRequest { limit = this@DeadLetterListCommand.limit })
        }
        val rows = results.flatMap { (node, response) ->
            response.deadLetters.map { dl ->
                buildList<Pair<String, Any?>> {
                    if (clustered) add("node" to node)
                    add("pushId" to dl.pushId.toB64())
                    add("session" to (dl.sessionId?.rawValue?.toB64() ?: ""))
                    add("backend" to backendLabel(dl.backend))
                    add("attempts" to dl.attempts)
                    add("reason" to dl.reason)
                    add("deadLetteredAt" to dl.deadLetteredAt)
                }
            }
        }
        emitRows(config.output, rows)
    }
}

/**
 * `push dead-letters retry` — PushAdmin.RetryDeadLetters. Fans out to every `--node`: a given pushId
 * lives on exactly one node's shard, and keymaster doesn't resolve which, so it asks all of them
 * (nodes without the id retry nothing). The reported count is the cluster-wide sum.
 */
class DeadLetterRetryCommand : CliktCommand(name = "retry") {
    private val config by requireObject<KeymasterConfig>()
    override fun help(context: Context) = "Move dead letters back onto the active queue with a fresh attempt budget."

    private val all by option("--all", help = "Retry every dead letter.").flag()
    private val ids by option("--id", help = "Specific dead-letter pushId (base64); repeatable.")
        .convert { decodeB64(it) }.multiple()

    override fun run() {
        if (all == ids.isNotEmpty()) {
            throw UsageError("Specify exactly one of --all or one/more --id.")
        }
        val results = fanOut(config.pushAdmins()) {
            it.client.retryDeadLetters(RetryDeadLettersRequest { if (!all) pushIds = ids })
        }
        emitCountAggregate(config, results, "retried") { it.retried }
    }
}

/** `push dead-letters purge` — PushAdmin.PurgeDeadLetters, fanned across every `--node` and summed. */
class DeadLetterPurgeCommand : CliktCommand(name = "purge") {
    private val config by requireObject<KeymasterConfig>()
    override fun help(context: Context) = "Permanently delete dead letters."

    private val all by option("--all", help = "Purge every dead letter.").flag()
    private val ids by option("--id", help = "Specific dead-letter pushId (base64); repeatable.")
        .convert { decodeB64(it) }.multiple()

    override fun run() {
        if (all == ids.isNotEmpty()) {
            throw UsageError("Specify exactly one of --all or one/more --id.")
        }
        val results = fanOut(config.pushAdmins()) {
            it.client.purgeDeadLetters(PurgeDeadLettersRequest { if (!all) pushIds = ids })
        }
        emitCountAggregate(config, results, "purged") { it.purged }
    }
}
