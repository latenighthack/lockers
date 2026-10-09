package com.latenighthack.lockers.connector

import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.internal.*
import com.latenighthack.lockers.room.v1.*
import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import java.io.File
import kotlin.test.*

class ArchiveMigrationSafetyTest {
    private suspend fun archive(scope: Int, alias: Int, epoch: Long, key: Secp256r1KeyPair): ArchivedRatchet {
        val public = Secp256R1Key.PublicKey(key.publicKey.encode())
        return ArchivedRatchet(PendingRatchet(PostLockerChangeRequest(roomId = RoomId(byteArrayOf(1)),
            lockerId = LockerId(byteArrayOf(scope.toByte()), LockerKeyspace(0)), writeRequestId = ByteArray(32) { alias.toByte() },
            ratchet = PostLockerChangeRequest.Ratchet(newPublicKey = public)), key.privateKey.encode()),
            LockState(locked = true, lockVersion = epoch, publicKey = public, scope = LockScope(
                kind = LockScopeKind.LOCK_SCOPE_LOCKER, keyspace = LockerKeyspace(0), lockerRawValue = byteArrayOf(scope.toByte()),
                unknownFields = byteArrayOf(0xa0.toByte(), 6, alias.toByte()))), epoch)
    }
    private suspend fun seed(file: File, rows: List<ArchivedRatchet>): DatabaseConfiguration {
        val target = ConnectorStorage.configuration(file.name)
        val old = target.copy(version = 4, stores = ConnectorStorage.definitionsV4.map { it.declaration },
            migrations = target.migrations.filter { it.toVersion <= 4 })
        val db = createDatabase(old, file.absolutePath); db.open()
        try { db.transaction(setOf(RatchetArchiveDefinitionV1.storeName)) {
            rows.forEach { save(RatchetArchiveDefinitionV1.storeName, RatchetArchiveDefinitionV1.encodeRow(it)) }
        } } finally { db.close() }
        return target
    }
    private class PausingDelegate(private val source: SqlStoreDelegate) :
        LifecycleStoreDelegate by source, ScopedStoreDelegate, IndexedQueryDelegate {
        override val supportsTransactions get() = source.supportsTransactions
        override suspend fun <T> transaction(block: suspend () -> T) = source.transaction(block)
        override suspend fun <T> transaction(lockKey: String, block: suspend () -> T) = source.transaction(lockKey, block)
        override suspend fun <T> transaction(stores: Set<String>, mode: TransactionMode, block: suspend () -> T) =
            source.transaction(stores, mode, block)
        override suspend fun count(tableName: String, query: IndexedQuery) = source.count(tableName, query)
        override suspend fun deleteBatch(tableName: String, query: IndexedQuery, identity: String, version: Int) =
            source.deleteBatch(tableName, query, identity, version)
        val secondPage = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        private var reads = 0
        override suspend fun query(tableName: String, query: IndexedQuery, identity: String, version: Int): QueryPage {
            if (tableName == RatchetArchiveDefinitionV1.storeName.value && ++reads == 2) {
                secondPage.complete(Unit); release.await()
            }
            return source.query(tableName, query, identity, version)
        }
    }
    @Test fun cancellationPreservesCommittedCursorAndReopenFinishesWithoutLegacyRescan() = runTest {
        val file = File.createTempFile("archive-resume", ".db")
        try {
            val key = Secp256r1KeyPair.generate()
            val rows = (1..9).map { archive(it, it, it.toLong(), key) }
            val target = seed(file, rows)
            val paused = PausingDelegate(SqlStoreDelegate(JdbcDriver(file.absolutePath, "sqlite"), "BLOB", target))
            val db = Database(target, paused); db.open()
            val migration = launch { LockerStoreImpl(db).archivedRatchets() }
            paused.secondPage.await(); migration.cancelAndJoin()
            db.transaction(setOf(ArchiveMigrationDefinitionV2.storeName), TransactionMode.READ_ONLY) {
                val status = ArchiveMigrationDefinitionV2.decode(getAll(ArchiveMigrationDefinitionV2.storeName).single() as ByteArray)
                assertEquals(4L, status.processed); assertFalse(status.complete)
            }
            db.close()
            val resumed = createDatabase(target, file.absolutePath); resumed.open()
            try {
                assertEquals(9, LockerStoreImpl(resumed).archivedRatchets().size)
                // A completed migration never consults legacy rows again, including removed legacy bytes.
                resumed.deleteBatch(RatchetArchiveDefinitionV1.storeName, IndexedQuery(RatchetArchiveDefinitionV1.room.key, 10_000))
                assertEquals(9, LockerStoreImpl(resumed).archivedRatchets().size)
                resumed.transaction(setOf(ArchiveMigrationDefinitionV2.storeName), TransactionMode.READ_ONLY) {
                    assertTrue(ArchiveMigrationDefinitionV2.decode(getAll(ArchiveMigrationDefinitionV2.storeName).single() as ByteArray).complete)
                }
            } finally { resumed.close() }
        } finally { file.delete() }
    }
    @Test fun sameEpochConflictCannotExposePartiallyMigratedKeysAndPreservesEveryV1Byte() = runTest {
        val file = File.createTempFile("archive-conflict", ".db")
        try {
            val first = Secp256r1KeyPair.generate(); val other = Secp256r1KeyPair.generate()
            val rows = (1..4).map { archive(it, it, it.toLong(), first) } +
                listOf(archive(5, 5, 5, first), archive(5, 6, 5, other))
            val bytes = rows.map { RatchetArchiveDefinitionV1.encodePayload(it) }
            val target = seed(file, rows)
            repeat(2) {
                val db = createDatabase(target, file.absolutePath); db.open()
                try {
                    val store = LockerStoreImpl(db)
                    assertFailsWith<IllegalArgumentException> { store.archivedRatchets() }
                    assertFailsWith<IllegalArgumentException> { store.hasArchivedRatchet(RoomId(byteArrayOf(1))) }
                    db.transaction(setOf(RatchetArchiveDefinitionV1.storeName, ArchiveMigrationDefinitionV2.storeName), TransactionMode.READ_ONLY) {
                        val original = getAll(RatchetArchiveDefinitionV1.storeName).map { it as ByteArray }
                        assertEquals(6, original.size)
                        bytes.forEach { expected -> assertTrue(original.any { it.contentEquals(expected) }) }
                        val status = ArchiveMigrationDefinitionV2.decode(getAll(ArchiveMigrationDefinitionV2.storeName).single() as ByteArray)
                        assertEquals(4L, status.processed); assertFalse(status.complete)
                    }
                } finally { db.close() }
            }
        } finally { file.delete() }
    }
    @Test fun wrongPrivateKeyFailsBeforeAnyArchiveCanBeAdopted() = runTest {
        val file = File.createTempFile("archive-private", ".db")
        try {
            val key = Secp256r1KeyPair.generate(); val other = Secp256r1KeyPair.generate()
            val valid = archive(1, 1, 1, key)
            val invalid = valid.copy(pending = valid.pending.copy(privateKey = other.privateKey.encode()))
            val target = seed(file, listOf(invalid))
            val db = createDatabase(target, file.absolutePath); db.open()
            try {
                assertFailsWith<IllegalArgumentException> { LockerStoreImpl(db).archivedRatchets() }
                db.transaction(setOf(RatchetArchiveDefinitionV1.storeName, RatchetArchiveDefinitionV2.storeName), TransactionMode.READ_ONLY) {
                    assertContentEquals(RatchetArchiveDefinitionV1.encodePayload(invalid), getAll(RatchetArchiveDefinitionV1.storeName).single() as ByteArray)
                    assertTrue(getAll(RatchetArchiveDefinitionV2.storeName).isEmpty())
                }
            } finally { db.close() }
        } finally { file.delete() }
    }
    @Test fun mutableOperationArgumentsAreFrozenBeforeMigrationSuspends() = runTest {
        for (operation in listOf("put", "remove", "hasRoom", "matching")) {
            val file = File.createTempFile("archive-freeze-$operation", ".db")
            try {
                val key = Secp256r1KeyPair.generate()
                val rows = (1..9).map { archive(it, it, it.toLong(), key) }
                val target = seed(file, rows)
                val paused = PausingDelegate(SqlStoreDelegate(JdbcDriver(file.absolutePath, "sqlite"), "BLOB", target))
                val db = Database(target, paused); db.open()
                try {
                    val store = LockerStoreImpl(db)
                    val candidate = if (operation == "put") archive(20, 20, 20, key) else rows[4]
                    val room = RoomId(byteArrayOf(1)); val public = key.publicKey.encode()
                    val originalPrivate = candidate.pending.privateKey.copyOf()
                    val call = async { when (operation) {
                        "put" -> { store.archiveRatchet(candidate); true }
                        "remove" -> { store.forgetArchivedRatchet(candidate); true }
                        "hasRoom" -> store.hasArchivedRatchet(room)
                        else -> RatchetArchive(db).matching(room, public) != null
                    } }
                    paused.secondPage.await()
                    candidate.pending.request.roomId!!.rawValue[0] = 99
                    candidate.pending.privateKey[0] = (candidate.pending.privateKey[0].toInt() xor 1).toByte()
                    candidate.publicKey[0] = 0
                    candidate.state!!.scope!!.lockerRawValue[0] = 99
                    room.rawValue[0] = 99; public[0] = 0
                    paused.release.complete(Unit)
                    assertTrue(call.await())
                    val adopted = store.archivedRatchets()
                    if (operation == "put") assertContentEquals(originalPrivate,
                        adopted.single { it.scope.lockerRawValue.contentEquals(byteArrayOf(20)) }.pending.privateKey)
                    if (operation == "remove") {
                        assertEquals(8, adopted.size)
                        assertFalse(adopted.any { it.scope.lockerRawValue.contentEquals(byteArrayOf(5)) })
                    }
                } finally { db.close() }
            } finally { file.delete() }
        }
    }

}
