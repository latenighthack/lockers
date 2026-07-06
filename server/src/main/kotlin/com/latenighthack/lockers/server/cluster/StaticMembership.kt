package com.latenighthack.lockers.server.cluster

import com.latenighthack.lockers.sharding.NodeId
import com.latenighthack.lockers.sharding.PeerAddress
import com.latenighthack.lockers.sharding.spi.Membership
import com.latenighthack.lockers.sharding.spi.PeerLocator
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow

/**
 * The VM/bare-metal blueprint's view of "who is in the cluster": a fixed node list from the
 * environment (`LOCKERS_PEERS`), this node's own id (`LOCKERS_NODE_ID`), and its peer-reachable
 * address (`LOCKERS_ADVERTISE_ADDR`). This mirrors a StatefulSet's stable ordinal set / a set of
 * distinct Railway services — the roster is static per deploy, and scaling is a redeploy that
 * changes `LOCKERS_PEERS` everywhere. (Dynamic membership is a k8s/etcd blueprint concern.)
 *
 * Parsing is pure and unit-tested via [ClusterTopology.fromEnv]; the [Membership] / [PeerLocator]
 * here are thin adapters over the parsed [ClusterTopology].
 */
class StaticMembership(private val topology: ClusterTopology) : Membership {
    override val self: NodeId = topology.self

    // The roster is fixed for the lifetime of the process, so `changes` is a single-value stream
    // that never completes — callers (ShardMapSource binders) simply see the one steady node set.
    private val roster = MutableStateFlow(topology.nodes)

    override fun current(): Set<NodeId> = roster.value
    override fun changes(): Flow<Set<NodeId>> = roster.asStateFlow()
}

/** Resolves a [NodeId] to its advertised [PeerAddress] from the same parsed [ClusterTopology]. */
class StaticPeerLocator(private val topology: ClusterTopology) : PeerLocator {
    override suspend fun addressOf(node: NodeId): PeerAddress? = topology.addressOf(node)
}

/**
 * The parsed, validated cluster roster. Pure data + a pure parser so the env→topology mapping is
 * fully unit-testable with zero infrastructure.
 *
 * `LOCKERS_PEERS` is a comma-separated list of `node=host:port` (explicit logical id) or bare
 * `host:port` (the `host` doubles as the node id — matches DNS/StatefulSet naming where the
 * hostname *is* the identity). This node's own entry is derived from [self] + [advertise].
 */
class ClusterTopology(
    val self: NodeId,
    private val addresses: Map<NodeId, PeerAddress>,
) {
    init {
        require(addresses.containsKey(self)) {
            "LOCKERS_NODE_ID '${self.value}' is not present in the peer set ${addresses.keys.map { it.value }}; " +
                "every node (including self) must appear in LOCKERS_PEERS"
        }
    }

    val nodes: Set<NodeId> get() = addresses.keys

    fun addressOf(node: NodeId): PeerAddress? = addresses[node]

    override fun equals(other: Any?): Boolean =
        other is ClusterTopology && self == other.self && addresses == other.addresses

    override fun hashCode(): Int = 31 * self.hashCode() + addresses.hashCode()

    override fun toString(): String = "ClusterTopology(self=${self.value}, addresses=$addresses)"

    companion object {
        /**
         * Builds the roster from the three env values. Returns `null` when sharding is not
         * configured (no peers) so the caller stays a monolith. Throws on malformed input so a
         * misconfigured cluster fails fast at boot rather than mis-routing silently.
         *
         * @param peers    `LOCKERS_PEERS`, e.g. `lockers-0=lockers-0.internal:8080,lockers-1=...`
         * @param nodeId   `LOCKERS_NODE_ID` — this node's logical id (falls back to hostname).
         * @param advertise `LOCKERS_ADVERTISE_ADDR` — this node's peer-reachable `host:port`.
         */
        fun fromEnv(peers: String?, nodeId: String?, advertise: String?): ClusterTopology? {
            if (peers.isNullOrBlank()) return null

            val addresses = LinkedHashMap<NodeId, PeerAddress>()
            for (raw in peers.split(',')) {
                val entry = raw.trim()
                if (entry.isEmpty()) continue
                val (id, address) = parseEntry(entry)
                val prior = addresses.put(id, address)
                require(prior == null) { "duplicate node id '${id.value}' in LOCKERS_PEERS" }
            }
            require(addresses.isNotEmpty()) { "LOCKERS_PEERS parsed to an empty node set: '$peers'" }

            val selfId = NodeId(
                nodeId?.trim()?.takeIf { it.isNotEmpty() }
                    ?: throw IllegalArgumentException(
                        "LOCKERS_NODE_ID must be set when LOCKERS_PEERS is set (self identity on the ring)"
                    )
            )

            // If self advertised an address, it must agree with (or supply) its peer-list entry.
            advertise?.trim()?.takeIf { it.isNotEmpty() }?.let { adv ->
                val advAddr = parseAddress(adv, "LOCKERS_ADVERTISE_ADDR")
                val existing = addresses[selfId]
                require(existing == null || existing == advAddr) {
                    "LOCKERS_ADVERTISE_ADDR '$adv' disagrees with self's LOCKERS_PEERS entry '$existing'"
                }
                addresses[selfId] = advAddr
            }

            return ClusterTopology(selfId, addresses)
        }

        private fun parseEntry(entry: String): Pair<NodeId, PeerAddress> {
            val eq = entry.indexOf('=')
            return if (eq >= 0) {
                val id = entry.substring(0, eq).trim()
                require(id.isNotEmpty()) { "empty node id in peer entry '$entry'" }
                NodeId(id) to parseAddress(entry.substring(eq + 1).trim(), entry)
            } else {
                // Bare host:port — the host is the logical id (DNS-name-as-identity).
                val address = parseAddress(entry, entry)
                NodeId(address.host) to address
            }
        }

        private fun parseAddress(value: String, context: String): PeerAddress {
            val colon = value.lastIndexOf(':')
            require(colon > 0 && colon < value.length - 1) {
                "malformed address '$value' in '$context' (expected host:port)"
            }
            val host = value.substring(0, colon)
            val port = value.substring(colon + 1).toIntOrNull()
                ?: throw IllegalArgumentException("invalid port in '$value' (in '$context')")
            require(port in 1..65535) { "port out of range in '$value' (in '$context')" }
            return PeerAddress(host, port)
        }
    }
}
