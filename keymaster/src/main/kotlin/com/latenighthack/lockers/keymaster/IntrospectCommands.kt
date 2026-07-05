package com.latenighthack.lockers.keymaster

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.ProgramResult
import com.github.ajalt.clikt.parameters.arguments.argument

/**
 * Scaffolding for session/room introspection. The server has no admin RPC for
 * these yet (sessions/rooms live behind internal stores only), so each leaf
 * reports what server-side RPC it would need and exits non-zero. Keeping the
 * commands in the tree makes the intended surface discoverable via `--help`.
 */
private fun CliktCommand.notImplemented(command: String, neededRpc: String): Nothing {
    echo("`$command` is not yet supported.", err = true)
    echo("It needs a server-side $neededRpc admin RPC, which the lockers server does not expose yet.", err = true)
    throw ProgramResult(2)
}

/** Parent `session` group (scaffold). */
class SessionCommand : CliktCommand(name = "session") {
    override fun help(context: Context) = "Inspect sessions (planned; needs a SessionAdmin RPC)."
    override fun run() = Unit
}

class SessionListCommand : CliktCommand(name = "list") {
    override fun help(context: Context) = "List active sessions (planned)."
    override fun run() = notImplemented("session list", "SessionAdmin.ListSessions")
}

class SessionGetCommand : CliktCommand(name = "get") {
    override fun help(context: Context) = "Inspect one session by id (planned)."
    private val sessionId by argument(name = "session-id", help = "Session id as base64.")
    override fun run() = notImplemented("session get $sessionId", "SessionAdmin.GetSession")
}

/** Parent `room` group (scaffold). */
class RoomCommand : CliktCommand(name = "room") {
    override fun help(context: Context) = "Inspect rooms and lockers (planned; needs a RoomAdmin RPC)."
    override fun run() = Unit
}

class RoomSubscribersCommand : CliktCommand(name = "subscribers") {
    override fun help(context: Context) = "List sessions subscribed to a room (planned)."
    private val roomId by argument(name = "room-id", help = "Room id as base64.")
    override fun run() = notImplemented("room subscribers $roomId", "RoomAdmin.ListSubscribers")
}

class RoomLockersCommand : CliktCommand(name = "lockers") {
    override fun help(context: Context) = "List lockers in a room (planned)."
    private val roomId by argument(name = "room-id", help = "Room id as base64.")
    override fun run() = notImplemented("room lockers $roomId", "RoomAdmin.ListLockers")
}
