package com.latenighthack.lockers.server

import com.latenighthack.lockers.observability.LockersTelemetry
import io.micrometer.core.instrument.MeterRegistry

import com.latenighthack.ktstore.*

/** Complete schemas must be composed before any shared handle is opened. */
object ServerStorage {
    val definitions: List<StoreDefinition<*>> = listOf(
        com.latenighthack.lockers.server.services.session.v1.SessionStoreImplDefinitionV1,
        com.latenighthack.lockers.server.services.session.v1.SessionInboxStoreImplDefinitionV1,
        com.latenighthack.lockers.server.services.push.v1.PushSessionStoreImplDefinitionV1,
        com.latenighthack.lockers.server.services.push.v1.PushQueueStoreImplDefinitionV1,
        com.latenighthack.lockers.server.services.push.v1.PushDeadLetterStoreImplDefinitionV1,
        com.latenighthack.lockers.server.services.room.v1.LockerStoreImplDefinitionV1,
        com.latenighthack.lockers.server.services.room.v1.RoomSequencesDefinitionV1("delivery"),
        com.latenighthack.lockers.server.services.room.v1.RoomSequencesDefinitionV1("push_delivery"),
        com.latenighthack.lockers.server.services.room.v1.WriteReceiptsDefinitionV1("delivery"),
        com.latenighthack.lockers.server.services.room.v1.WriteReceiptsDefinitionV1("push_delivery"),
        com.latenighthack.lockers.server.services.room.v1.DeliveryOutboxStoreDefinitionV1("delivery"),
        com.latenighthack.lockers.server.services.room.v1.DeliveryOutboxStoreDefinitionV1("push_delivery"),
        com.latenighthack.lockers.server.services.room.v1.LockStoreImplDefinitionV1,
        com.latenighthack.lockers.server.services.room.v1.SubscriptionStoreImplDefinitionV1,
    )
    fun configuration(identity: String, additional: List<StoreDefinition<*>> = emptyList()) =
        definitionDatabaseConfiguration(identity, definitions + additional)
    fun inMemory(identity: String = "ServerStorage-test", meterRegistry: MeterRegistry? = null, telemetry: LockersTelemetry = LockersTelemetry.NONE) =
        Database(configuration(identity + "-${kotlin.random.Random.nextLong()}"), com.latenighthack.lockers.server.tools.MeasuredStoreDelegate(InMemoryStoreDelegate(), meterRegistry, telemetry))
    fun postgres(location: String, additional: List<StoreDefinition<*>> = emptyList(), meterRegistry: MeterRegistry? = null, telemetry: LockersTelemetry = LockersTelemetry.NONE) =
        createPostgresDatabase(configuration("lockers-server", additional), location, { com.latenighthack.lockers.server.tools.MeasuredStoreDelegate(it, meterRegistry, telemetry) })
}
