package com.latenighthack.lockers.server

import com.latenighthack.ktstore.Database
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.agents.LockerAgentRegistry
import com.latenighthack.lockers.server.services.room.v1.*
import com.latenighthack.lockers.server.services.session.v1.*
import com.latenighthack.lockers.server.storage.v1.*
import com.latenighthack.lockers.session.v1.*
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.*

/** Controlled latency fixture, not a claim about production or iPhone rendering. */
class DeliveryLatencyTest {
    @Test fun reportsAcknowledgementAndDerivedDeliverySeparately() = runBlocking {
        for (outboxEnabled in listOf(false, true)) {
            val db = com.latenighthack.lockers.server.ServerStorage.inMemory()
            val outbox = DeliveryOutboxStore(db).also { it.prepareStores() }
            val lockers = LockerStoreImpl(db).also { it.prepare() }
            val locks = LockStoreImpl(db).also { it.prepare() }
            val subs = SubscriptionStoreImpl(db).also { it.prepare() }
            db.open()
            val room = RoomId(ByteArray(32) { 1 })
            val sourceId = LockerId(ByteArray(32) { 2 }, LockerKeyspace(99))
            val derivedId = LockerId(ByteArray(32) { 3 }, LockerKeyspace(100))
            for (i in 0 until 100) subs.addSubscription(ServerSessionId(byteArrayOf(i.toByte())), ServerRoomId(room.rawValue))
            var arrival = CompletableDeferred<Long>()
            val gatewayCalls = AtomicInteger()
            val gateway = object : SessionGatewayService {
                private fun delivered(request: PostEventRequest) {
                    if (request.event?.locker?.lockerId == derivedId) arrival.complete(System.nanoTime())
                }
                override suspend fun postEvent(request: PostEventRequest): PostEventResponse {
                    gatewayCalls.incrementAndGet(); delay(20); delivered(request); return PostEventResponse()
                }
                override suspend fun postEvents(request: PostEventsRequest): PostEventsResponse {
                    gatewayCalls.incrementAndGet(); delay(20); request.groups.forEach { delivered(it) }
                    return PostEventsResponse(request.groups.map { PostEventResponse() })
                }
            }
            val discovery = object : SessionGatewayDiscovery {
                override suspend fun findServer(sessionId: SessionId): SessionGatewayService = error("must use bulk resolution")
                override suspend fun resolveGroups(sessionIds: List<SessionId>) = sessionIds.groupBy { it.rawValue[0].toInt() % 2 }.values.map { SessionGatewayGroup(it, gateway) }
            }
            val agentTimes = mutableListOf<Double>()
            val agent = object : LockerAgentRegistry {
                override suspend fun processPayload(roomId: RoomId, lockerId: LockerId, locker: Locker): List<LockerAgentRegistry.LockerWrite> {
                    val start = System.nanoTime(); delay(20)
                    val result = listOf(LockerAgentRegistry.LockerWrite(derivedId, Locker { open { encodedPayload = ByteArray(1_145_000) } }))
                    agentTimes.add((System.nanoTime() - start) / 1e6)
                    return result
                }
            }
            val service = RoomServiceImpl(subs, lockers, locks, discovery, object : RoomOwnership {
                override suspend fun resolve(keyspace: Long, roomId: RoomId) = RoomOwner.Local()
            }, agent, SimpleMeterRegistry(), LockersConfig.defaults().copy(deliveryOutboxEnabled = outboxEnabled, maxLockerPayloadBytes = 2 * 1024 * 1024), outbox)
            service.start()
            val rpc = LocalRoomServiceRpc(service)
            val acknowledgements = mutableListOf<Double>(); val deliveries = mutableListOf<Double>(); val overhead = mutableListOf<Double>()
            try {
                var version = 0L
                repeat(12) { n ->
                    arrival = CompletableDeferred()
                    val start = System.nanoTime()
                    val response = rpc.postLockerChange(PostLockerChangeRequest(roomId = room, lockerId = sourceId,
                        locker = Locker { open { encodedPayload = byteArrayOf(n.toByte()) } }, parentVersion = version,
                        writeRequestId = ByteArray(16) { n.toByte() }))
                    val ackMs = (System.nanoTime() - start) / 1e6
                    assertTrue(response.result.isOk())
                    version = response.version
                    val frameMs = (withTimeout(5_000) { arrival.await() } - start) / 1e6
                    if (n >= 2) { acknowledgements.add(ackMs); deliveries.add(frameMs); overhead.add(ackMs - agentTimes.last()) }
                }
                fun List<Double>.percentile(p: Double) = sorted()[((size - 1) * p).toInt()]
                println("LATENCY_FIXTURE outbox=$outboxEnabled subscribers=100 gateways=2 bytes=1145000 samples=10 ack_p50_ms=${acknowledgements.percentile(.5)} ack_p95_ms=${acknowledgements.percentile(.95)} non_agent_p50_ms=${overhead.percentile(.5)} gateway_frame_p50_ms=${deliveries.percentile(.5)} gateway_frame_p95_ms=${deliveries.percentile(.95)} gateway_calls=${gatewayCalls.get()}")
            } finally { service.closeAndJoin(); db.close() }
        }
    }
}
