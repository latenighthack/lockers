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

/** Parent `push` group — dispatches to its subcommands. */
class PushCommand : CliktCommand(name = "push") {
    override fun help(context: Context) = "Inspect and recover the push delivery queue (PushAdmin)."
    override fun run() = Unit
}

/** `push stats` — PushAdmin.GetQueueStats. */
class PushStatsCommand : CliktCommand(name = "stats") {
    private val config by requireObject<KeymasterConfig>()
    override fun help(context: Context) = "Show queue and dead-letter depths, total and per backend."
    override fun run() {
        val stats = runBlocking { config.pushAdmin().getQueueStats(GetQueueStatsRequest { }) }
        val fields = buildList {
            add("queued" to stats.queued)
            add("deadLettered" to stats.deadLettered)
            stats.queuedByBackend.forEach { add("queued.${backendLabel(it.backend)}" to it.count) }
            stats.deadLetteredByBackend.forEach { add("deadLettered.${backendLabel(it.backend)}" to it.count) }
        }
        emit(config.output, fields)
    }
}

/** `push drain` — PushAdmin.DrainQueue. */
class PushDrainCommand : CliktCommand(name = "drain") {
    private val config by requireObject<KeymasterConfig>()
    override fun help(context: Context) = "Re-feed every persisted-but-idle queued push into the processor."
    override fun run() {
        val response = runBlocking { config.pushAdmin().drainQueue(DrainQueueRequest { }) }
        emit(config.output, listOf("drained" to response.drained))
    }
}

/** Parent `push dead-letters` group. */
class DeadLetterCommand : CliktCommand(name = "dead-letters") {
    override fun help(context: Context) = "Inspect and manage parked (dead-lettered) pushes."
    override fun run() = Unit
}

/** `push dead-letters list` — PushAdmin.ListDeadLetters. */
class DeadLetterListCommand : CliktCommand(name = "list") {
    private val config by requireObject<KeymasterConfig>()
    override fun help(context: Context) = "List parked pushes; copy a pushId into retry/purge."

    private val limit by option("--limit", help = "Max rows; <= 0 uses the server default.").int().default(0)

    override fun run() {
        val response = runBlocking {
            config.pushAdmin().listDeadLetters(ListDeadLettersRequest { limit = this@DeadLetterListCommand.limit })
        }
        val rows = response.deadLetters.map { dl ->
            listOf<Pair<String, Any?>>(
                "pushId" to dl.pushId.toB64(),
                "session" to (dl.sessionId?.rawValue?.toB64() ?: ""),
                "backend" to backendLabel(dl.backend),
                "attempts" to dl.attempts,
                "reason" to dl.reason,
                "deadLetteredAt" to dl.deadLetteredAt,
            )
        }
        emitRows(config.output, rows)
    }
}

/** `push dead-letters retry` — PushAdmin.RetryDeadLetters. */
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
        val response = runBlocking {
            config.pushAdmin().retryDeadLetters(
                RetryDeadLettersRequest { if (!all) pushIds = ids }
            )
        }
        emit(config.output, listOf("retried" to response.retried))
    }
}

/** `push dead-letters purge` — PushAdmin.PurgeDeadLetters. */
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
        val response = runBlocking {
            config.pushAdmin().purgeDeadLetters(
                PurgeDeadLettersRequest { if (!all) pushIds = ids }
            )
        }
        emit(config.output, listOf("purged" to response.purged))
    }
}
