package com.latenighthack.lockers.connector

import androidx.test.platform.app.InstrumentationRegistry
import com.latenighthack.ktstore.createDatabase
import kotlinx.coroutines.test.runTest
import java.io.File
import kotlin.test.Test

class AndroidStorageContractTest {
    @Test fun sqliteConnectorStoreSurvivesCloseAndReopen() = runTest {
        val file = File.createTempFile("lockers-contract", ".db",
            InstrumentationRegistry.getInstrumentation().targetContext.cacheDir)
        val configuration = ConnectorStorage.configuration(file.name)
        try { verifyPersistentConnectorStorage { createDatabase(configuration, file.absolutePath) } }
        finally { file.delete() }
    }
    @Test fun historicalSqliteSchemasPreservePrivateBytesAndNormalizeAliases() = runTest {
        for (version in 3..5) {
            val file = File.createTempFile("lockers-migration-v$version", ".db",
                InstrumentationRegistry.getInstrumentation().targetContext.cacheDir)
            try { verifyHistoricalConnectorMigration(file.name, version) { createDatabase(it, file.absolutePath) } }
            finally { file.delete() }
        }
    }

}
