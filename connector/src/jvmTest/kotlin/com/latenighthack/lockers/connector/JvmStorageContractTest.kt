package com.latenighthack.lockers.connector

import com.latenighthack.ktstore.createDatabase
import kotlinx.coroutines.test.runTest
import java.io.File
import kotlin.test.Test

class JvmStorageContractTest {
    @Test fun sqliteConnectorStoreSurvivesCloseAndReopen() = runTest {
        val file = File.createTempFile("lockers-contract", ".db")
        val configuration = ConnectorStorage.configuration(file.name)
        try { verifyPersistentConnectorStorage { createDatabase(configuration, file.absolutePath) } }
        finally { file.delete() }
    }
}
