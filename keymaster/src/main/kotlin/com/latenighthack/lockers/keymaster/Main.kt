package com.latenighthack.lockers.keymaster

import com.github.ajalt.clikt.core.main
import com.github.ajalt.clikt.core.subcommands
import kotlin.system.exitProcess

/**
 * ktbuf's HttpRpcClient owns an OkHttpClient whose non-daemon "OkHttp Dispatcher"
 * thread parks for a 60s keep-alive after each call, so a plain return would leave
 * the JVM alive long after the result is printed. ktbuf exposes no way to close that
 * client, so we exit explicitly once the command finishes. Clikt's own `main()`
 * already calls exitProcess for help, usage errors, and ProgramResult, so only the
 * success path and uncaught RPC/IO errors reach this wrapper.
 */
fun main(args: Array<String>) {
    try {
        keymaster().main(args)
    } catch (t: Throwable) {
        System.err.println(t.message ?: t.toString())
        exitProcess(1)
    }
    exitProcess(0)
}

private fun keymaster() = Keymaster().subcommands(
    BroadcastCommand(),
    PushCommand().subcommands(
        PushStatsCommand(),
        PushDrainCommand(),
        DeadLetterCommand().subcommands(
            DeadLetterListCommand(),
            DeadLetterRetryCommand(),
            DeadLetterPurgeCommand(),
        ),
    ),
    HealthCommand(),
    MetricsCommand(),
    SessionCommand().subcommands(
        SessionListCommand(),
        SessionGetCommand(),
    ),
    RoomCommand().subcommands(
        RoomSubscribersCommand(),
        RoomLockersCommand(),
    ),
)
