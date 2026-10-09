package com.latenighthack.lockers.connector

import com.latenighthack.ktstore.createDatabase
import kotlinx.coroutines.test.runTest
import java.io.File
import kotlin.test.Test

class ArchiveMigrationTest {
    @Test fun historicalSqliteSchemasPreservePrivateBytesAndNormalizeAliases() = runTest {
        for (version in 3..5) {
            val file = File.createTempFile("connector-v$version", ".db")
            try { verifyHistoricalConnectorMigration(file.name, version) { createDatabase(it, file.absolutePath) } }
            finally { file.delete() }
        }
    }
}
