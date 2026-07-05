package com.latenighthack.lockers.keymaster

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.UsageError
import com.github.ajalt.clikt.core.requireObject
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.multiple
import com.github.ajalt.clikt.parameters.options.option
import com.latenighthack.lockers.broadcast.v1.BroadcastRequest
import com.latenighthack.lockers.common.v1.SessionId
import kotlinx.coroutines.runBlocking

/**
 * `broadcast` — enqueue a notification into sessions via BroadcastAdmin.Broadcast.
 * Exactly one target must be given: `--all` (every active session) or one or more
 * `--session <base64 id>`. The notification carries a push (`--title`/`--body`)
 * and/or an opaque `--payload`.
 */
class BroadcastCommand : CliktCommand(name = "broadcast") {
    private val config by requireObject<KeymasterConfig>()

    override fun help(context: Context) = "Enqueue a broadcast notification into sessions."

    private val all by option("--all", help = "Broadcast to every active session.").flag()

    private val sessions by option(
        "--session",
        help = "Target session id as base64; repeat for multiple sessions.",
    ).convert { decodeB64(it) }.multiple()

    private val title by option("--title", help = "Push notification title.")
    private val body by option("--body", help = "Push notification body.")
    private val payload by option(
        "--payload",
        help = "Opaque payload delivered in the event, as base64.",
    ).convert { decodeB64(it) }

    override fun run() {
        if (all == sessions.isNotEmpty()) {
            throw UsageError("Specify exactly one target: --all or one/more --session.")
        }
        if (payload == null && title == null && body == null) {
            throw UsageError("Nothing to send: provide --payload and/or --title/--body.")
        }

        val response = runBlocking {
            config.broadcastAdmin().broadcast(
                BroadcastRequest {
                    notification {
                        this@BroadcastCommand.payload?.let { bytes -> this.payload { rawValue = bytes } }
                        if (this@BroadcastCommand.title != null || this@BroadcastCommand.body != null) {
                            push {
                                title = this@BroadcastCommand.title ?: ""
                                body = this@BroadcastCommand.body ?: ""
                            }
                        }
                    }
                    target {
                        if (all) {
                            kind.all { }
                        } else {
                            kind.sessions {
                                sessionIds = sessions.map { raw -> SessionId { rawValue = raw } }
                            }
                        }
                    }
                }
            )
        }

        emit(config.output, listOf("delivered" to response.delivered))
    }
}
