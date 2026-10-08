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
}
