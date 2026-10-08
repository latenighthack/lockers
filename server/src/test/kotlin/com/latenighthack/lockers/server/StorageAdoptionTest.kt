package com.latenighthack.lockers.server

import com.latenighthack.ktstore.*
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.test.*

/** Frozen V1 wire fixtures, including an unknown field and legacy Android blob TEXT values. */
class StorageAdoptionTest {
    private fun hex(value: String) = value.chunked(2).map { it.toInt(16).toByte() }.toByteArray()
    private val fixtures = mapOf(
        "sessions" to StoreRow(hex("0a030a0101a00607"), listOf(BoundStoreKey.SerializedKey("sessionIdtoByteArray", hex("0a0101")))),
        "inbox" to StoreRow(hex("0a030a01011a030a0102a00607"), listOf(BoundStoreKey.SerializedKey("sessionIdtoByteArray", hex("0a0101")), BoundStoreKey.SerializedKey("eventIdtoByteArray", hex("0a0102")))),
        "push_session" to StoreRow(hex("0a030a0101a00607"), listOf(BoundStoreKey.SerializedKey("sessionIdtoByteArray", hex("0a0101")))),
        "push" to StoreRow(hex("0a030a0101a00607"), listOf(BoundStoreKey.SerializedKey("pushIdtoByteArray", hex("0a0101")))),
        "push_deadletter" to StoreRow(hex("0a030a0101a00607"), listOf(BoundStoreKey.SerializedKey("pushIdtoByteArray", hex("0a0101")))),
        "subscriptions" to StoreRow(hex("0a030a010112030a0102a00607"), listOf(BoundStoreKey.SerializedKey("sessionIdtoByteArray", hex("0a0101")), BoundStoreKey.SerializedKey("roomIdtoByteArray", hex("0a0102")))),
        "lockers" to StoreRow(hex("0a030a010110031a030a0102a00607"), listOf(BoundStoreKey.SerializedKey("roomIdtoByteArray", hex("0a0101")), BoundStoreKey.SerializedKey("lockerIdtoByteArray", hex("0a0102")), BoundStoreKey.LongKey("keyspace", 3L))),
        "locks" to StoreRow(hex("0a030a01011002180322030a0104a00607"), listOf(BoundStoreKey.SerializedKey("roomIdtoByteArray", hex("0a0101")), BoundStoreKey.SerializedKey("lockerIdtoByteArray", hex("0a0104")), BoundStoreKey.LongKey("scopeKind", 2L), BoundStoreKey.LongKey("keyspace", 3L))),
        "delivery_outbox" to StoreRow(hex("0a030a010112030a0102a00607"), listOf(BoundStoreKey.SerializedKey("eventIdtoByteArray", hex("0a0101")), BoundStoreKey.SerializedKey("roomIdtoByteArray", hex("0a0102")))),
        "delivery_write_receipts" to StoreRow(hex("0a010112030a0102a00607"), listOf(BoundStoreKey.SerializedKey("requestId", hex("01")), BoundStoreKey.SerializedKey("roomIdtoByteArray", hex("0a0102")))),
        "delivery_room_sequences" to StoreRow(hex("0a030a0101a00607"), listOf(BoundStoreKey.SerializedKey("roomIdtoByteArray", hex("0a0101")))),
        "push_delivery_outbox" to StoreRow(hex("0a030a010112030a0102a00607"), listOf(BoundStoreKey.SerializedKey("eventIdtoByteArray", hex("0a0101")), BoundStoreKey.SerializedKey("roomIdtoByteArray", hex("0a0102")))),
        "push_delivery_write_receipts" to StoreRow(hex("0a010112030a0102a00607"), listOf(BoundStoreKey.SerializedKey("requestId", hex("01")), BoundStoreKey.SerializedKey("roomIdtoByteArray", hex("0a0102")))),
        "push_delivery_room_sequences" to StoreRow(hex("0a030a0101a00607"), listOf(BoundStoreKey.SerializedKey("roomIdtoByteArray", hex("0a0101"))))
    )
    @Test fun adoptsEveryOwnedStorePreservesBytesAndSupportsReopen() = runBlocking {
        val file = File.createTempFile("serverstorage-legacy", ".db")
        val configuration = ServerStorage.configuration(file.name)
        val legacy = SqlStoreDelegate(JdbcDriver(file.absolutePath, "sqlite"), "BLOB", legacyBinaryText = true)
        try {
            ServerStorage.legacyDefinitionsV3.map { it.declaration }.forEach { legacy.registerStore(it.name.value, it.keys, it.primaryKey) }
            legacy.createStores()
            fixtures.forEach { (table, row) -> legacy.save(table, row.data, row.keys) }
        } finally { legacy.close() }
        fun handle() = createDatabase(configuration, file.absolutePath)
        var current = handle()
        try {
            current.open()
            for (attempt in 1..2) {
                current.transaction(configuration.stores.map { it.name }.toSet(), TransactionMode.READ_ONLY) {
                    fixtures.forEach { (table, expected) ->
                        assertContentEquals(expected.data as ByteArray, getAll(StoreName(table)).single() as ByteArray)
                        // Every independently captured scalar key remains queryable after rebuilding.
                        expected.keys.forEach { key ->
                            assertContentEquals(expected.data as ByteArray, get(StoreName(table), StoreRelation.Eq(key)) as ByteArray)
                        }
                    }
                }
                current.close()
                if (attempt == 1) { current = handle(); current.open() }
            }
        } finally { current.close(); file.delete() }
    }
}
