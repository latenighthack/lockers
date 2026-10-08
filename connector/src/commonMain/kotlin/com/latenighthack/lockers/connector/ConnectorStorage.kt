package com.latenighthack.lockers.connector

import com.latenighthack.ktstore.*

/** Complete schemas must be composed before any shared handle is opened. */
object ConnectorStorage {
    val definitions: List<StoreDefinition<*>> = listOf(
        com.latenighthack.lockers.connector.PushRegistrationStoreImplDefinitionV1,
        com.latenighthack.lockers.connector.SubscriptionStoreImplDefinitionV1,
        com.latenighthack.lockers.connector.SessionStoreImplDefinitionV1,
        com.latenighthack.lockers.connector.internal.LockerStoreImplDefinitionV1,
        com.latenighthack.lockers.connector.internal.RatchetJournalDefinitionV1,
        com.latenighthack.lockers.connector.internal.ConnectorEventJournalDefinitionV1,
        PushIntentDefinitionV2,
    )
    fun configuration(identity: String, additional: List<StoreDefinition<*>> = emptyList()) =
        definitionDatabaseConfiguration(identity, definitions + additional)
    fun inMemory(identity: String = "ConnectorStorage-test") =
        Database(configuration(identity), InMemoryStoreDelegate())
}
