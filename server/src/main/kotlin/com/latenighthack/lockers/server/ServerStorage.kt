package com.latenighthack.lockers.server

import com.latenighthack.lockers.observability.LockersTelemetry
import io.micrometer.core.instrument.MeterRegistry

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.server.services.push.v1.PushCredentialDefinitionV2
import com.latenighthack.lockers.server.services.push.v1.PushDeadLetterIndexDefinitionV2
import com.latenighthack.lockers.server.services.push.v1.PushDeadLetterStoreImplDefinitionV1
import com.latenighthack.lockers.server.services.push.v1.PushQueueStoreImplDefinitionV1
import com.latenighthack.lockers.server.services.push.v1.PushRetentionDefinitionV3
import com.latenighthack.lockers.server.services.push.v1.PushSessionStoreImplDefinitionV1
import com.latenighthack.lockers.server.services.push.v1.PushWorkDefinitionV2
import com.latenighthack.lockers.server.services.push.v1.PushWorkDefinitionV3
import com.latenighthack.lockers.server.services.room.v1.AgentWorkDefinitionV2
import com.latenighthack.lockers.server.services.room.v1.AgentWorkDefinitionV3
import com.latenighthack.lockers.server.services.room.v1.DeliveryOutboxStoreDefinitionV1
import com.latenighthack.lockers.server.services.room.v1.LockStoreImplDefinitionV1
import com.latenighthack.lockers.server.services.room.v1.LockerStoreDefinitionV2
import com.latenighthack.lockers.server.services.room.v1.LockerStoreImplDefinitionV1
import com.latenighthack.lockers.server.services.room.v1.OutboxEntriesDefinitionV2
import com.latenighthack.lockers.server.services.room.v1.OutboxEntriesDefinitionV3
import com.latenighthack.lockers.server.services.room.v1.OutboxHeadsDefinitionV2
import com.latenighthack.lockers.server.services.room.v1.RoomSequencesDefinitionV1
import com.latenighthack.lockers.server.services.room.v1.SnapshotDefinitionV2
import com.latenighthack.lockers.server.services.room.v1.SubscriptionIntentDefinitionV2
import com.latenighthack.lockers.server.services.room.v1.SubscriptionStoreImplDefinitionV1
import com.latenighthack.lockers.server.services.room.v1.WriteReceiptsDefinitionV1
import com.latenighthack.lockers.server.services.session.v1.InboxByteLedgerDefinitionV2
import com.latenighthack.lockers.server.services.session.v1.InboxDeliveryReceiptDefinitionV2
import com.latenighthack.lockers.server.services.session.v1.RevokedSessionAuthorityDefinitionV2
import com.latenighthack.lockers.server.services.session.v1.RevokedSessionDefinitionV2
import com.latenighthack.lockers.server.services.session.v1.SessionInboxMetadataDefinitionV2
import com.latenighthack.lockers.server.services.session.v1.SessionInboxStoreDefinitionV2
import com.latenighthack.lockers.server.services.session.v1.SessionInboxStoreDefinitionV3
import com.latenighthack.lockers.server.services.session.v1.SessionInboxStoreImplDefinitionV1
import com.latenighthack.lockers.server.services.session.v1.SessionStoreImplDefinitionV1
import com.latenighthack.lockers.server.services.session.v1.UsedSessionProofDefinitionV2
import com.latenighthack.lockers.server.services.session.v1.UsedSessionProofOwnersDefinitionV2

/** Complete schemas must be composed before any shared handle is opened. */
object ServerStorage {
    private const val OPERATIONS_SCHEMA_VERSION = 6
    val legacyDefinitionsV3: List<StoreDefinition<*>> = listOf(
        SessionStoreImplDefinitionV1,
        SessionInboxStoreImplDefinitionV1,
        PushSessionStoreImplDefinitionV1,
        PushQueueStoreImplDefinitionV1,
        PushDeadLetterStoreImplDefinitionV1,
        LockerStoreImplDefinitionV1,
        RoomSequencesDefinitionV1("delivery"),
        RoomSequencesDefinitionV1("push_delivery"),
        WriteReceiptsDefinitionV1("delivery"),
        WriteReceiptsDefinitionV1("push_delivery"),
        DeliveryOutboxStoreDefinitionV1("delivery"),
        DeliveryOutboxStoreDefinitionV1("push_delivery"),
        LockStoreImplDefinitionV1,
        SubscriptionStoreImplDefinitionV1,
    )
    val additionsV4: List<StoreDefinition<*>> = listOf(
        AgentWorkDefinitionV2,
        OutboxEntriesDefinitionV2("delivery"),
        OutboxHeadsDefinitionV2("delivery"),
        OutboxEntriesDefinitionV2("push_delivery"),
        OutboxHeadsDefinitionV2("push_delivery"),
        InboxByteLedgerDefinitionV2,

        SnapshotDefinitionV2,
        PushWorkDefinitionV2,
        PushRetentionDefinitionV3,
        PushDeadLetterIndexDefinitionV2,
        PushCredentialDefinitionV2,
        SessionInboxMetadataDefinitionV2,
        InboxDeliveryReceiptDefinitionV2,
        UsedSessionProofDefinitionV2,
        UsedSessionProofOwnersDefinitionV2,
        RevokedSessionDefinitionV2,
        RevokedSessionAuthorityDefinitionV2,
    )
    val definitionsV4 = legacyDefinitionsV3.map { definition ->
        if (definition === SessionInboxStoreImplDefinitionV1)
            SessionInboxStoreDefinitionV2
        else if (definition === LockerStoreImplDefinitionV1)
            LockerStoreDefinitionV2 else definition
    } + additionsV4
    val definitionsV5 = definitionsV4 + SubscriptionIntentDefinitionV2
    val definitions = definitionsV5.map { definition ->
        when (definition) {
            SessionInboxStoreDefinitionV2 -> SessionInboxStoreDefinitionV3
            PushWorkDefinitionV2 -> PushWorkDefinitionV3
            AgentWorkDefinitionV2 -> AgentWorkDefinitionV3
            else -> when (definition.storeName.value) {
                "delivery_outbox_entries_v2" -> OutboxEntriesDefinitionV3("delivery")
                "push_delivery_outbox_entries_v2" -> OutboxEntriesDefinitionV3("push_delivery")
                else -> definition
            }
        }
    }
    fun configuration(identity: String, additional: List<StoreDefinition<*>> = emptyList()): DatabaseConfiguration {
        val historical = definitionDatabaseConfiguration(identity, legacyDefinitionsV3 + additional)
        val previous = (definitionsV4 + additional).map { it.declaration }
        val version5 = (definitionsV5 + additional).map { it.declaration }
        val declarations = (definitions + additional).map { it.declaration }
        return historical.copy(version = OPERATIONS_SCHEMA_VERSION, stores = declarations, migrations =
            historical.migrations +
            DatabaseMigration.configured(3, 4, historical.stores, previous) {
                for (definition in additionsV4) createStore(definition.declaration)
                val inbox = SessionInboxStoreDefinitionV2
                rebuildStore(inbox.storeName, inbox.declaration) { raw ->
                    StoreRow(raw.copyOf(), inbox.encodeRow(inbox.decode(raw)).keys)
                }
                val lockers = LockerStoreDefinitionV2
                rebuildStore(lockers.storeName, lockers.declaration) { raw ->
                    StoreRow(raw.copyOf(), lockers.encodeRow(lockers.decode(raw)).keys)
                }
            } + DatabaseMigration.configured(4, 5, previous, version5) {
                createStore(SubscriptionIntentDefinitionV2.declaration)
            } + DatabaseMigration.configured(5, OPERATIONS_SCHEMA_VERSION, version5, declarations) {
                for (definition in definitions) {
                    val old = definitionsV5.single { it.storeName == definition.storeName }
                    if (old.declaration != definition.declaration) rebuildStore(definition.storeName,
                        definition.declaration) { raw ->
                        @Suppress("UNCHECKED_CAST")
                        val typed = definition as StoreDefinition<Any>
                        StoreRow(raw.copyOf(), typed.encodeRow(typed.decode(raw)).keys)
                    }
                }
            })
    }
    fun inMemory(identity: String = "ServerStorage-test", meterRegistry: MeterRegistry? = null, telemetry: LockersTelemetry = LockersTelemetry.NONE) =
        Database(configuration(identity + "-${kotlin.random.Random.nextLong()}"), com.latenighthack.lockers.server.tools.MeasuredStoreDelegate(com.latenighthack.lockers.server.services.room.v1.FencedMemoryDelegate(InMemoryStoreDelegate()), meterRegistry, telemetry))
    fun postgres(location: String, additional: List<StoreDefinition<*>> = emptyList(), meterRegistry: MeterRegistry? = null, telemetry: LockersTelemetry = LockersTelemetry.NONE): Database {
        val configuration = configuration("lockers-server", additional).copy(externalTables = setOf("room_claim", "room_claim_capacity", "session_gateway", "shard_map", "shard_fence"))
        val driver = com.latenighthack.lockers.server.services.room.v1.FencedSqlDriver(JdbcDriver(location.removePrefix("jdbc:postgresql:"), "postgresql"))
        return Database(configuration, com.latenighthack.lockers.server.tools.MeasuredStoreDelegate(SqlStoreDelegate(driver, "BYTEA", configuration), meterRegistry, telemetry))
    }
}
