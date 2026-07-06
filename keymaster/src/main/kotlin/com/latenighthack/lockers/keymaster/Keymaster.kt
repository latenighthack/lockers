package com.latenighthack.lockers.keymaster

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.context
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.multiple
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.choice

/**
 * Resolved global configuration, shared with every subcommand via the Clikt
 * context object. URLs point at the two server listeners: [adminUrl] is the
 * internal admin port (BroadcastAdmin / PushAdmin), [publicUrl] the client port
 * that also serves the HTTP health/metrics probes.
 *
 * [adminNodes] holds the cluster's admin endpoints for fan-out commands. It is the
 * explicit `--node` list when given, otherwise the single [adminUrl] (so a monolith
 * and a single-node invocation behave exactly as before). Keymaster deliberately does
 * NOT call `GetTopology`: the operator supplies the node list, keeping admin ops
 * decoupled from the (parallel) topology-RPC milestone.
 */
data class KeymasterConfig(
    val adminUrl: String,
    val adminNodes: List<String>,
    val publicUrl: String,
    val adminToken: String?,
    val output: OutputFormat,
)

/**
 * Root command. Holds the global options and publishes a [KeymasterConfig] onto
 * the context so subcommands can `requireObject<KeymasterConfig>()`.
 *
 * `autoEnvvarPrefix = "KEYMASTER"` gives every option an automatic environment
 * fallback (e.g. `--admin-url` <- `KEYMASTER_ADMIN_URL`), matching the server's
 * 12-factor config. Precedence is flag > environment > default.
 */
class Keymaster : CliktCommand(name = "keymaster") {
    init {
        context { autoEnvvarPrefix = "KEYMASTER" }
    }

    override fun help(context: Context) = "Admin CLI to control and observe the lockers server."

    private val adminUrl by option(
        "--admin-url",
        help = "Base URL of the internal admin port (BroadcastAdmin / PushAdmin).",
    ).default("http://localhost:8081")

    private val adminNodes by option(
        "--node",
        help = "A cluster node's admin URL; repeat to target every node. Push admin ops fan out " +
            "across all --node endpoints and aggregate. Defaults to --admin-url when omitted.",
    ).multiple()

    private val publicUrl by option(
        "--public-url",
        help = "Base URL of the public port (serves /healthz, /readyz, /metrics).",
    ).default("http://localhost:8080")

    private val adminToken by option(
        "--admin-token",
        help = "Value sent as the x-admin-token header; required when the server sets LOCKERS_ADMIN_TOKEN.",
    )

    private val output by option(
        "--output",
        help = "Output format.",
    ).choice("table" to OutputFormat.TABLE, "json" to OutputFormat.JSON).default(OutputFormat.TABLE)

    override fun run() {
        val nodes = adminNodes.ifEmpty { listOf(adminUrl) }
        currentContext.findOrSetObject { KeymasterConfig(adminUrl, nodes, publicUrl, adminToken, output) }
    }
}
