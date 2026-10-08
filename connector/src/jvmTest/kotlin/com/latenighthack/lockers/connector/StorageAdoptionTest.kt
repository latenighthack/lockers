package com.latenighthack.lockers.connector

import com.latenighthack.ktstore.*
import kotlinx.coroutines.runBlocking
import java.io.File
import kotlin.test.*

/** Frozen V1 wire fixtures, including an unknown field and legacy Android blob TEXT values. */
class StorageAdoptionTest {
    private fun hex(value: String) = value.chunked(2).map { it.toInt(16).toByte() }.toByteArray()
    private val fixtures = mapOf(
        "acks" to StoreRow(hex("0a0101120102a00607"), listOf(BoundStoreKey.SerializedKey("roomIdRawValuebyteArrayIdentity", hex("01")), BoundStoreKey.SerializedKey("eventIdRawValuebyteArrayIdentity", hex("02")))),
        "subscriptions" to StoreRow(hex("0a01011801a00607"), listOf(BoundStoreKey.SerializedKey("roomIdRawValuebyteArrayIdentity", hex("01")))),
        "lockers" to StoreRow(hex("0a01011201021803220104a00607"), listOf(BoundStoreKey.SerializedKey("roomIdRawValuebyteArrayIdentity", hex("01")), BoundStoreKey.SerializedKey("lockerIdRawValuebyteArrayIdentity", hex("02")), BoundStoreKey.LongKey("lockerKeyspace", 3L))),
        "push_registrations" to StoreRow(hex("08011201041801a00607"), listOf(BoundStoreKey.SerializedKey("backendintToBytes", hex("00000001"))))
    )
    @Test fun adoptsEveryOwnedStorePreservesBytesAndSupportsReopen() = runBlocking {
        val file = File.createTempFile("connectorstorage-legacy", ".db")
        val configuration = ConnectorStorage.configuration(file.name)
        val legacy = SqlStoreDelegate(JdbcDriver(file.absolutePath, "sqlite"), "BLOB", legacyBinaryText = true)
        try {
            configuration.stores.forEach { legacy.registerStore(it.name.value, it.keys, it.primaryKey) }
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
