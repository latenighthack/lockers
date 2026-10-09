package com.latenighthack.lockers.connector

import com.latenighthack.ktstore.*

/** Complete schemas must be composed before any shared handle is opened.
 * Ratchet stores contain private keys: production applications must use trusted confidential storage,
 * with encryption/access/backup policy appropriate to their key provider. Cache and event APIs do not expose them.
 */
object ConnectorStorage {
    /** Frozen configured schema published before the additive recovery stores. */
    val definitionsV3: List<StoreDefinition<*>> = listOf(
        PushRegistrationStoreImplDefinitionV1, SubscriptionStoreImplDefinitionV1,
        SessionStoreImplDefinitionV1, com.latenighthack.lockers.connector.internal.LockerStoreImplDefinitionV1,
    )
    val definitions: List<StoreDefinition<*>> = listOf(
        com.latenighthack.lockers.connector.PushRegistrationStoreImplDefinitionV1,
        com.latenighthack.lockers.connector.SubscriptionStoreImplDefinitionV1,
        com.latenighthack.lockers.connector.SessionStoreImplDefinitionV2,
        com.latenighthack.lockers.connector.internal.LockerStoreImplDefinitionV1,
        com.latenighthack.lockers.connector.internal.RatchetJournalDefinitionV1,
        com.latenighthack.lockers.connector.internal.RatchetArchiveDefinitionV1,
        com.latenighthack.lockers.connector.internal.ConnectorEventJournalDefinitionV1,
        PushIntentDefinitionV2,
        AckConfirmationAgeDefinitionV1,
        com.latenighthack.lockers.connector.internal.JournalRetentionDefinitionV1,
    )
    fun configuration(identity: String, additional: List<StoreDefinition<*>> = emptyList()): DatabaseConfiguration {
        val historical = definitionDatabaseConfiguration(identity, definitionsV3 + additional)
        val target = (definitions + additional).map { it.declaration }
        return historical.copy(version = 4, stores = target, migrations = historical.migrations +
            DatabaseMigration.configured(3, 4, historical.stores, target) {
                rebuildStore(SessionStoreImplDefinitionV1.storeName, SessionStoreImplDefinitionV2.declaration) { bytes ->
                    val row = SessionStoreImplDefinitionV2.encodeRow(SessionStoreImplDefinitionV1.decode(bytes))
                    StoreRow(bytes.copyOf(), row.keys)
                }
                for (definition in definitions.filter { it.storeName !in definitionsV3.map { old -> old.storeName } }) {
                    createStore(definition.declaration)
                }
            })
    }
    fun inMemory(identity: String = "ConnectorStorage-test") =
        Database(configuration(identity), InMemoryStoreDelegate())
}
