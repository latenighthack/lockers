package com.latenighthack.lockers.server.cluster

import assertk.assertThat
import assertk.assertions.containsExactlyInAnyOrder
import assertk.assertions.hasMessage
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import assertk.assertions.messageContains
import com.latenighthack.lockers.sharding.NodeId
import com.latenighthack.lockers.sharding.PeerAddress
import kotlin.test.Test
import kotlin.test.assertFailsWith

/** Pure-parse coverage for the static blueprint's env->topology mapping (zero infra). */
class ClusterTopologyTest {
    @Test
    fun nullPeersMeansNoCluster() {
        assertThat(ClusterTopology.fromEnv(peers = null, nodeId = "a", advertise = null)).isNull()
        assertThat(ClusterTopology.fromEnv(peers = "  ", nodeId = "a", advertise = null)).isNull()
    }

    @Test
    fun parsesExplicitNodeEqualsAddressEntries() {
        val topo = ClusterTopology.fromEnv(
            peers = "lockers-0=lockers-0.internal:8080, lockers-1=lockers-1.internal:9090",
            nodeId = "lockers-0",
            advertise = null,
        )
        assertThat(topo).isNotNull()
        topo!!
        assertThat(topo.self).isEqualTo(NodeId("lockers-0"))
        assertThat(topo.nodes).containsExactlyInAnyOrder(NodeId("lockers-0"), NodeId("lockers-1"))
        assertThat(topo.addressOf(NodeId("lockers-0"))).isEqualTo(PeerAddress("lockers-0.internal", 8080))
        assertThat(topo.addressOf(NodeId("lockers-1"))).isEqualTo(PeerAddress("lockers-1.internal", 9090))
    }

    @Test
    fun bareHostPortUsesHostAsNodeId() {
        val topo = ClusterTopology.fromEnv(peers = "10.0.0.1:8080,10.0.0.2:8080", nodeId = "10.0.0.1", advertise = null)!!
        assertThat(topo.nodes).containsExactlyInAnyOrder(NodeId("10.0.0.1"), NodeId("10.0.0.2"))
        assertThat(topo.addressOf(NodeId("10.0.0.2"))).isEqualTo(PeerAddress("10.0.0.2", 8080))
    }

    @Test
    fun advertiseAddressAgreeingWithSelfEntryIsAccepted() {
        // ADVERTISE_ADDR may be set redundantly (common in templated deploys) as long as it agrees
        // with self's entry in LOCKERS_PEERS.
        val topo = ClusterTopology.fromEnv(
            peers = "n0=n0.internal:8080,n1=n1.internal:8080",
            nodeId = "n0",
            advertise = "n0.internal:8080",
        )!!
        assertThat(topo.addressOf(NodeId("n0"))).isEqualTo(PeerAddress("n0.internal", 8080))
    }

    @Test
    fun advertiseAddressDisagreementFailsFast() {
        val e = assertFailsWith<IllegalArgumentException> {
            ClusterTopology.fromEnv(
                peers = "n0=n0.internal:8080,n1=n1.internal:8080",
                nodeId = "n0",
                advertise = "somewhere-else:8080",
            )
        }
        assertThat(e).messageContains("disagrees")
    }

    @Test
    fun ipv6HostWithPortParses() {
        // lastIndexOf(':') keeps IPv6 colons in the host and splits only the trailing port.
        val topo = ClusterTopology.fromEnv(peers = "n0=fd00::1:8080", nodeId = "n0", advertise = null)!!
        assertThat(topo.addressOf(NodeId("n0"))).isEqualTo(PeerAddress("fd00::1", 8080))
    }

    @Test
    fun selfMustAppearInPeers() {
        val e = assertFailsWith<IllegalArgumentException> {
            ClusterTopology.fromEnv(peers = "n1=n1.internal:8080", nodeId = "n0", advertise = null)
        }
        assertThat(e).messageContains("must appear in LOCKERS_PEERS")
    }

    @Test
    fun missingNodeIdWithPeersFailsFast() {
        assertFailsWith<IllegalArgumentException> {
            ClusterTopology.fromEnv(peers = "n0=n0.internal:8080", nodeId = null, advertise = null)
        }
    }

    @Test
    fun duplicateNodeIdFailsFast() {
        val e = assertFailsWith<IllegalArgumentException> {
            ClusterTopology.fromEnv(peers = "n0=a:8080,n0=b:8080", nodeId = "n0", advertise = null)
        }
        assertThat(e).messageContains("duplicate")
    }

    @Test
    fun malformedAddressFailsFast() {
        assertFailsWith<IllegalArgumentException> {
            ClusterTopology.fromEnv(peers = "n0=hostwithoutport", nodeId = "n0", advertise = null)
        }
        assertFailsWith<IllegalArgumentException> {
            ClusterTopology.fromEnv(peers = "n0=host:notaport", nodeId = "n0", advertise = null)
        }
        assertFailsWith<IllegalArgumentException> {
            ClusterTopology.fromEnv(peers = "n0=host:70000", nodeId = "n0", advertise = null)
        }
    }

    @Test
    fun peerEntryConstructorRejectsSelfAbsent() {
        val e = assertFailsWith<IllegalArgumentException> {
            ClusterTopology(NodeId("ghost"), mapOf(NodeId("real") to PeerAddress("h", 8080)))
        }
        assertThat(e).hasMessage(
            "LOCKERS_NODE_ID 'ghost' is not present in the peer set [real]; " +
                "every node (including self) must appear in LOCKERS_PEERS"
        )
    }
}
