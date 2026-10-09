package com.latenighthack.lockers.connector.test

import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.connector.storage.v1.*
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.test.*

class ReviewSubscriptionRevisionPersistenceTests {
    @Test fun `SQLite V6 adoption and V7 reopen retain removal floors and frozen subscription metadata`() = runBlocking {
        val file = File.createTempFile("connector-intent", ".db")
        val target = ConnectorStorage.configuration(file.name)
        val legacy = target.copy(version = 6, stores = ConnectorStorage.definitionsV6.map { it.declaration }, migrations = target.migrations.filter { it.toVersion <= 6 })
        val row = StoredSubscription(roomIdRawValue = byteArrayOf(2), unknownFields = byteArrayOf(0x98.toByte(), 6, 1))
        var database = createDatabase(legacy, file.absolutePath)
        try {
            database.open()
            database.transaction(setOf(SubscriptionStoreImplDefinitionV1.storeName)) {
                save(SubscriptionStoreImplDefinitionV1.storeName, SubscriptionStoreImplDefinitionV1.encodeRow(row))
            }
            database.close(); database = createDatabase(target, file.absolutePath); database.open()
            var store = SubscriptionStoreImpl(database)
            val room = RoomId(byteArrayOf(2))
            assertEquals(1L, store.ensureIntentRevision(room, true))
            assertContentEquals(row.toByteArray(), store.getSubscription(room)!!.toByteArray())
            assertEquals(2L, store.commitIntent(room, false)); assertContentEquals(row.unknownFields, store.getSubscription(room)!!.unknownFields)
            store.deleteSubscription(room)
            database.close(); database = createDatabase(target, file.absolutePath); database.open(); store = SubscriptionStoreImpl(database)
            assertTrue(store.getAllSubscriptions().isEmpty()); assertEquals(2L, store.intentRevision(room)!!.revision)
            assertEquals(3L, store.commitIntent(room, true))
            assertFailsWith<SubscriptionRevisionConflictException> { store.commitIntent(room, false, expectedRevision = 2, minimumRevision = 100) }
            assertEquals(3L, store.intentRevision(room)!!.revision)
        } finally { database.close(); file.delete() }
    }
}
