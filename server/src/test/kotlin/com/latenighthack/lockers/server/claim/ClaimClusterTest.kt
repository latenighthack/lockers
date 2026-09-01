package com.latenighthack.lockers.server.claim

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isNotEmpty
import assertk.assertions.isNotNull
import assertk.assertions.isNull
import assertk.assertions.isTrue
import com.latenighthack.lockers.common.v1.Locker
import com.latenighthack.lockers.common.v1.LockerId
import com.latenighthack.lockers.common.v1.LockerKeyspace
import com.latenighthack.lockers.common.v1.RoomId
import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.room.v1.PostLockerChangeRequest
import com.latenighthack.lockers.room.v1.PostLockerChangeResponse
import com.latenighthack.lockers.room.v1.RoomServiceRpc
import com.latenighthack.lockers.server.agents.LOBBY_KEYSPACE
import com.latenighthack.lockers.server.agents.LockerAgentRegistry
import com.latenighthack.lockers.server.services.room.v1.SubscriptionStoreImpl
import com.latenighthack.lockers.server.storage.v1.ServerRoomId
import com.latenighthack.lockers.server.storage.v1.ServerSessionId
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test

/**
 * The design doc's integration matrix (`docs/design/claim-ownership.md` §Test plan) on the
 * two-node harness: two real MonolithComponents in claim mode over loopback HTTP, sharing one
 * store delegate and one claim substrate. Test TTL 500ms / renew 100ms (tunable-duration idiom).
 */
class ClaimClusterTest {
    private fun room(name: String) = RoomId(name.encodeToByteArray())

    private fun post(roomName: String, ks: Long = 1L, lockerRaw: Byte = 9) = PostLockerChangeRequest {
        roomId = room(roomName)
        lockerId = LockerId {
            rawValue = byteArrayOf(lockerRaw)
            keyspace = LockerKeyspace { value = ks }
        }
        locker = Locker { open { encodedPayload = byteArrayOf(1) } }
    }

    private suspend fun awaitUntil(timeoutMs: Long = 5_000, condition: suspend () -> Boolean) {
        withTimeout(timeoutMs) {
            while (!condition()) delay(20)
        }
    }

    /**
     * Retries writes to [roomName] until one returns OK. Each attempt targets a fresh locker id so
     * the locker-version CAS (which rejects a re-post of an already-written locker with
     * `UPDATE_LOCAL_VERSION`) never masks the ownership result under test.
     */
    private suspend fun awaitOkWrite(node: ClaimNode, roomName: String, timeoutMs: Long = 5_000) {
        var attempt = 0
        withTimeout(timeoutMs) {
            while (true) {
                val request = post(roomName, lockerRaw = (100 + attempt).toByte())
                attempt++
                val response = runCatching { node.roomClient().postLockerChange(request) }.getOrNull()
                if (response?.result is PostLockerChangeResponse.Result.OK) return@withTimeout
                delay(50)
            }
        }
    }

    @Test
    fun `write forwarding - non-owner forwards to the owner and relays its response`() = runBlocking {
        startTwoNodeClaimCluster().use { cluster ->
            val first = cluster.node1.roomClient().postLockerChange(post("r1"))
            assertThat(first.result is PostLockerChangeResponse.Result.OK).isTrue()

            // A plain client's write on the non-owner is proxied east-west to the owner and the
            // owner's OK relayed back; ownership does not move.
            val second = cluster.node2.roomClient().postLockerChange(post("r1", lockerRaw = 10))
            assertThat(second.result is PostLockerChangeResponse.Result.OK).isTrue()
            assertThat(cluster.roomClaims.lookup(room("r1"))!!.nodeId).isEqualTo("node1")
            assertThat(
                cluster.node2.meterRegistry.find("lockers.room.forward.writes").counter()?.count()
            ).isEqualTo(1.0)

            // Loop protection: a forwarded-stamped call is never forwarded again — the non-owner
            // answers NOT_OWNER + redirect (the smart-routing-client contract, unchanged).
            val stamped = RoomServiceRpc(cluster.node2.rpc) { _, _ -> mapOf("fwd" to "1") }
            val third = stamped.postLockerChange(post("r1", lockerRaw = 11))
            assertThat(third.result is PostLockerChangeResponse.Result.NOT_OWNER).isTrue()
            assertThat(third.redirect?.ownerAddress).isNotNull()
            assertThat(third.redirect!!.ownerAddress).isNotEmpty()
            assertThat(third.redirect!!.ownerAddress).isEqualTo(cluster.node1.addr)
        }
    }

    @Test
    fun `single agent - the game agent runs only on the owner node`() = runBlocking {
        val invocations = mutableMapOf<String, AtomicInteger>()
        val cluster = startTwoNodeClaimCluster(configureCore = { nodeId, core ->
            val counter = AtomicInteger(0)
            invocations[nodeId] = counter
            core.overrideAgentRegistry = object : LockerAgentRegistry {
                override suspend fun processPayload(
                    roomId: RoomId,
                    lockerId: LockerId,
                    locker: Locker,
                ): List<LockerAgentRegistry.LockerWrite> {
                    counter.incrementAndGet()
                    return emptyList()
                }
            }
        })
        cluster.use {
            // node1 claims the room; interleaved writes from both sides.
            assertThat(
                it.node1.roomClient().postLockerChange(post("game", LOBBY_KEYSPACE)).result
                    is PostLockerChangeResponse.Result.OK
            ).isTrue()
            repeat(3) { i ->
                // Non-owner write: forwarded to the owner — the only node where the agent runs.
                val forwarded = it.node2.roomClient().postLockerChange(post("game", LOBBY_KEYSPACE, lockerRaw = i.toByte()))
                assertThat(forwarded.result is PostLockerChangeResponse.Result.OK).isTrue()
                // Owner write (fresh locker id: the forwarded write above already landed its id).
                val ok = it.node1.roomClient().postLockerChange(post("game", LOBBY_KEYSPACE, lockerRaw = (50 + i).toByte()))
                assertThat(ok.result is PostLockerChangeResponse.Result.OK).isTrue()
            }
            // 1 initial + 3 forwarded + 3 owner-direct, all processed on node1 only.
            assertThat(invocations["node1"]!!.get()).isEqualTo(7)
            assertThat(invocations["node2"]!!.get()).isEqualTo(0)
        }
    }

    @Test
    fun `cross-node fan-out - events reach a subscriber homed on the other node`() = runBlocking {
        startTwoNodeClaimCluster().use { cluster ->
            val subscriber = SessionId("subscriber-1".encodeToByteArray())
            // The subscriber's WebSocket lives on node2 (what ClaimSessionRegistry.attach records)…
            cluster.sessionGateways.upsert(subscriber, "node2", cluster.node2.addr, ttlMs = 60_000)
            // …and it is durably subscribed to the room (what the subscription RPC records).
            val subs = SubscriptionStoreImpl(cluster.delegate).also { it.prepare() }
            subs.addSubscription(
                ServerSessionId(subscriber.rawValue),
                ServerRoomId(room("fanout").rawValue),
            )

            // Writing on node1 must deliver over the registry-discovered gateway to node2.
            val response = cluster.node1.roomClient().postLockerChange(post("fanout"))
            assertThat(response.result is PostLockerChangeResponse.Result.OK).isTrue()

            awaitUntil {
                cluster.node2.meterRegistry.find("lockers.session.events.posted").counter()?.count() == 1.0
            }
            // And node1 processed it as a remote session, not a local one.
            assertThat(
                cluster.node1.meterRegistry.find("lockers.session.events.posted").counter()?.count() ?: 0.0
            ).isEqualTo(0.0)
        }
    }

    @Test
    fun `failover - a crashed owner's rooms are stolen after TTL expiry`() = runBlocking {
        startTwoNodeClaimCluster().use { cluster ->
            assertThat(
                cluster.node1.roomClient().postLockerChange(post("failover")).result
                    is PostLockerChangeResponse.Result.OK
            ).isTrue()
            assertThat(cluster.roomClaims.lookup(room("failover"))!!.epoch).isEqualTo(1L)

            cluster.node1.crash()

            // Writes via node2 redirect until the claim expires (≤TTL), then steal + succeed.
            awaitOkWrite(cluster.node2, "failover")
            val row = cluster.roomClaims.lookup(room("failover"))!!
            assertThat(row.nodeId).isEqualTo("node2")
            assertThat(row.epoch).isEqualTo(2L)

            // Subsequent writes keep working on the new owner.
            val next = cluster.node2.roomClient().postLockerChange(post("failover", lockerRaw = 42))
            assertThat(next.result is PostLockerChangeResponse.Result.OK).isTrue()
        }
    }

    @Test
    fun `graceful drain - a stopped node's rooms re-claim with no TTL wait`() = runBlocking {
        startTwoNodeClaimCluster().use { cluster ->
            assertThat(
                cluster.node1.roomClient().postLockerChange(post("drain")).result
                    is PostLockerChangeResponse.Result.OK
            ).isTrue()

            cluster.node1.stopGracefully()
            assertThat(cluster.roomClaims.lookup(room("drain"))).isNull()

            // First write on node2 claims immediately — fresh insert, no expiry wait, epoch 1.
            val response = cluster.node2.roomClient().postLockerChange(post("drain"))
            assertThat(response.result is PostLockerChangeResponse.Result.OK).isTrue()
            assertThat(cluster.roomClaims.lookup(room("drain"))!!.epoch).isEqualTo(1L)
        }
    }

    @Test
    fun `fencing - a node partitioned from the claim store demotes and never resurrects`() = runBlocking {
        val failingByNode = mutableMapOf<String, ToggleableFailingRoomClaimStore>()
        val cluster = startTwoNodeClaimCluster(roomClaimsForNode = { nodeId, shared ->
            if (nodeId == "node1") {
                ToggleableFailingRoomClaimStore(shared).also { failingByNode[nodeId] = it }
            } else {
                shared
            }
        })
        cluster.use {
            assertThat(
                it.node1.roomClient().postLockerChange(post("fenced")).result
                    is PostLockerChangeResponse.Result.OK
            ).isTrue()

            // Partition node1 from the claim store: renew fails, and after a streak > TTL it
            // demotes everything. Its writes now fail (resolve cannot reach the store). Each probe
            // targets a fresh locker so the version CAS can't mask the ownership outcome.
            failingByNode["node1"]!!.failing.set(true)
            var probe = 0
            awaitUntil {
                val request = post("fenced", lockerRaw = (60 + probe).toByte())
                probe++
                val response = runCatching { it.node1.roomClient().postLockerChange(request) }
                response.isFailure ||
                    response.getOrNull()?.result is PostLockerChangeResponse.Result.NOT_OWNER
            }

            // node2 steals once the unrenewed claim expires.
            awaitOkWrite(it.node2, "fenced")
            assertThat(it.roomClaims.lookup(room("fenced"))!!.nodeId).isEqualTo("node2")

            // Heal the partition: node1 resolves node2 as owner and forwards the write there —
            // served OK without resurrecting node1's ownership.
            failingByNode["node1"]!!.failing.set(false)
            val healed = it.node1.roomClient().postLockerChange(post("fenced", lockerRaw = 43))
            assertThat(healed.result is PostLockerChangeResponse.Result.OK).isTrue()
            assertThat(it.roomClaims.lookup(room("fenced"))!!.nodeId).isEqualTo("node2")
        }
    }
}
