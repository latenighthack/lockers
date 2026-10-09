package com.latenighthack.lockers.connector

import com.latenighthack.ktstore.createDatabase
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.coroutines.test.runTest
import kotlin.random.Random
import kotlin.test.Test

/** Browser-only: Node has no IndexedDB; the Node test run explicitly excludes this class. */
class BrowserStorageContractTest {
    @Test fun indexedDbConnectorStoreSurvivesCloseAndReopen() = runTest { withContext(Dispatchers.Default) {
        val configuration = ConnectorStorage.configuration("lockers-contract-${Random.nextLong()}")
        verifyPersistentConnectorStorage { createDatabase(configuration, null) }
    } }
    @Test fun historicalIndexedDbSchemasPreservePrivateBytesAndNormalizeAliases() = runTest { withContext(Dispatchers.Default) {
        for (version in 3..6) {
            val identity = "lockers-migration-v$version-${Random.nextLong()}"
            verifyHistoricalConnectorMigration(identity, version) { createDatabase(it, null) }
        }
    } }

    @Test fun sdkMutationsCommitRollbackAndReopenIndexedDb() = runTest { withContext(Dispatchers.Default) {
        val configuration = ConnectorStorage.configuration("lockers-mutations-${Random.nextLong()}")
        verifyPersistentConnectorMutations { createDatabase(configuration, null) }
    } }

}
