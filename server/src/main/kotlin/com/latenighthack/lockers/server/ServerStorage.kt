package com.latenighthack.lockers.server

import com.latenighthack.lockers.observability.LockersTelemetry
import io.micrometer.core.instrument.MeterRegistry

import com.latenighthack.ktstore.*

/** Complete schemas must be composed before any shared handle is opened. */
object ServerStorage {
    val legacyDefinitionsV3: List<StoreDefinition<*>> = listOf(
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
    val additionsV4: List<StoreDefinition<*>> = listOf(
        com.latenighthack.lockers.server.services.push.v1.PushWorkDefinitionV2,
        com.latenighthack.lockers.server.services.push.v1.PushCredentialDefinitionV2,
        com.latenighthack.lockers.server.services.session.v1.SessionInboxMetadataDefinitionV2,
        com.latenighthack.lockers.server.services.session.v1.UsedSessionProofDefinitionV2,
        com.latenighthack.lockers.server.services.session.v1.UsedSessionProofOwnersDefinitionV2,
        com.latenighthack.lockers.server.services.session.v1.RevokedSessionDefinitionV2,
    )
    val definitions = legacyDefinitionsV3 + additionsV4
    fun configuration(identity: String, additional: List<StoreDefinition<*>> = emptyList()): DatabaseConfiguration {
        val historical = definitionDatabaseConfiguration(identity, legacyDefinitionsV3 + additional)
        val declarations = (definitions + additional).map { it.declaration }
        return historical.copy(version = 4, stores = declarations, migrations = historical.migrations +
            DatabaseMigration.configured(3, 4, historical.stores, declarations) {
                for (definition in additionsV4) createStore(definition.declaration)
            })
    }
    fun inMemory(identity: String = "ServerStorage-test", meterRegistry: MeterRegistry? = null, telemetry: LockersTelemetry = LockersTelemetry.NONE) =
        Database(configuration(identity + "-${kotlin.random.Random.nextLong()}"), com.latenighthack.lockers.server.tools.MeasuredStoreDelegate(com.latenighthack.lockers.server.services.room.v1.FencedMemoryDelegate(InMemoryStoreDelegate()), meterRegistry, telemetry))
    fun postgres(location: String, additional: List<StoreDefinition<*>> = emptyList(), meterRegistry: MeterRegistry? = null, telemetry: LockersTelemetry = LockersTelemetry.NONE): Database {
        val configuration = configuration("lockers-server", additional).copy(externalTables = setOf("room_claim", "session_gateway", "shard_map", "shard_fence"))
        val driver = com.latenighthack.lockers.server.services.room.v1.FencedSqlDriver(JdbcDriver(location.removePrefix("jdbc:postgresql:"), "postgresql"))
        return Database(configuration, com.latenighthack.lockers.server.tools.MeasuredStoreDelegate(SqlStoreDelegate(driver, "BYTEA", configuration), meterRegistry, telemetry))
    }
}
