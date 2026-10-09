package com.latenighthack.lockers.connector

import com.latenighthack.ktstore.createDatabase
import kotlinx.coroutines.test.runTest
import platform.Foundation.NSTemporaryDirectory
import kotlin.random.Random
import kotlin.test.Test

class AppleStorageContractTest {
    @Test fun sqliteConnectorStoreSurvivesCloseAndReopen() = runTest {
        val location = NSTemporaryDirectory() + "lockers-contract-${Random.nextLong()}.db"
        val configuration = ConnectorStorage.configuration(location)
        verifyPersistentConnectorStorage { createDatabase(configuration, location) }
    }
    @Test fun historicalSqliteSchemasPreservePrivateBytesAndNormalizeAliases() = runTest {
        for (version in 3..6) {
            val location = NSTemporaryDirectory() + "lockers-migration-v$version-${Random.nextLong()}.db"
            verifyHistoricalConnectorMigration(location, version) { createDatabase(it, location) }
        }
    }

    @Test fun sdkMutationsCommitRollbackAndReopenSqlite() = runTest {
        val location = NSTemporaryDirectory() + "lockers-mutations-${Random.nextLong()}.db"
        val configuration = ConnectorStorage.configuration(location)
        verifyPersistentConnectorMutations { createDatabase(configuration, location) }
    }

}
