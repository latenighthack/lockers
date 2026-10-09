package com.latenighthack.lockers.connector

import com.latenighthack.ktcrypto.*
import com.latenighthack.ktstore.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.internal.*
import com.latenighthack.lockers.connector.storage.v1.*
import com.latenighthack.lockers.room.v1.*
import kotlin.test.*

/** Runs against the actual platform driver, not an in-memory imitation of migration. */
suspend fun verifyHistoricalConnectorMigration(
    identity: String,
    version: Int,
    factory: (DatabaseConfiguration) -> Database,
) {
    require(version in 3..5)
    val target = ConnectorStorage.configuration(identity)
    val definitions = when (version) {
        3 -> ConnectorStorage.definitionsV3
        4 -> ConnectorStorage.definitionsV4
        else -> ConnectorStorage.definitionsV4 + RatchetExpectationDefinitionV2
    }
    val legacy = target.copy(version = version, stores = definitions.map { it.declaration },
        migrations = target.migrations.filter { it.toVersion <= version })
    val unknown = byteArrayOf(0xa0.toByte(), 6, 7)
    val ack = StoredAck(byteArrayOf(1), byteArrayOf(2), unknownFields = unknown)
    val cache = StoredLocker(roomIdRawValue = byteArrayOf(1), lockerIdRawValue = byteArrayOf(2),
        lockerKeyspace = 4, lockerPayload = byteArrayOf(3), version = 11, unknownFields = unknown)
    val ackDefinition = if (version == 3) SessionStoreImplDefinitionV1 else SessionStoreImplDefinitionV2
    val ackBytes = ackDefinition.encodePayload(ack)
    val cacheBytes = LockerStoreImplDefinitionV1.encodePayload(cache)
    val keys = List(4) { Secp256r1KeyPair.generate() }
    suspend fun archive(index: Int): ArchivedRatchet {
        val public = Secp256R1Key.PublicKey(keys[index].publicKey.encode())
        val scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_LOCKER,
            keyspace = LockerKeyspace(4, unknownFields = byteArrayOf(0xa0.toByte(), 6, index.toByte())),
            lockerRawValue = byteArrayOf(2), unknownFields = byteArrayOf(0xa0.toByte(), 6, index.toByte()))
        return ArchivedRatchet(PendingRatchet(PostLockerChangeRequest(roomId = RoomId(byteArrayOf(1)),
            lockerId = LockerId(byteArrayOf(2), LockerKeyspace(4)), writeRequestId = ByteArray(32) { (index + 1).toByte() },
            ratchet = PostLockerChangeRequest.Ratchet(newPublicKey = public), unknownFields = unknown),
            keys[index].privateKey.encode()), LockState(locked = true, scope = scope,
            lockVersion = index + 2L, publicKey = public, unknownFields = unknown), 100 - index.toLong())
    }
    val older = archive(0); val newer = archive(1)
    val legacyAuthority = archive(2).let { value -> value.copy(
        pending = value.pending.copy(request = value.pending.request.copy(
            lockerId = LockerId(byteArrayOf(3), LockerKeyspace(4)))),
        state = LockState(locked = true, publicKey = value.state!!.publicKey, unknownFields = unknown)) }
    val terminalAuthority = archive(3).let { value -> value.copy(
        pending = value.pending.copy(request = value.pending.request.copy(
            lockerId = LockerId(byteArrayOf(4), LockerKeyspace(4)))),
        state = value.state!!.copy(lockVersion = Long.MAX_VALUE, scope = value.state.scope!!.copy(lockerRawValue = byteArrayOf(4)))) }
    val originals = listOf(older, newer, legacyAuthority, terminalAuthority)
    val archiveBytes = originals.map { RatchetArchiveDefinitionV1.encodePayload(it) }
    val pendingBytes = RatchetJournalDefinitionV1.encodePayload(newer.pending)
    val expectation = RatchetExpectation(newer.pending.request.writeRequestId, older.state!!,
        sourceUncertaintyAcknowledged = true)
    val expectationBytes = RatchetExpectationDefinitionV2.encodePayload(expectation)
    var db = factory(legacy); db.open()
    try {
        val names = mutableSetOf(ackDefinition.storeName, LockerStoreImplDefinitionV1.storeName)
        if (version >= 4) names += setOf(RatchetArchiveDefinitionV1.storeName, RatchetJournalDefinitionV1.storeName)
        if (version == 5) names += RatchetExpectationDefinitionV2.storeName
        db.transaction(names) {
            save(ackDefinition.storeName, StoreRow(ackBytes, ackDefinition.encodeRow(ack).keys))
            save(LockerStoreImplDefinitionV1.storeName, StoreRow(cacheBytes, LockerStoreImplDefinitionV1.encodeRow(cache).keys))
            if (version >= 4) {
                originals.forEachIndexed { index, value ->
                    save(RatchetArchiveDefinitionV1.storeName,
                        StoreRow(archiveBytes[index], RatchetArchiveDefinitionV1.encodeRow(value).keys))
                }
                save(RatchetJournalDefinitionV1.storeName,
                    StoreRow(pendingBytes, RatchetJournalDefinitionV1.encodeRow(newer.pending).keys))
                if (version == 5) save(RatchetExpectationDefinitionV2.storeName,
                    StoreRow(expectationBytes, RatchetExpectationDefinitionV2.encodeRow(expectation).keys))
            }
        }
    } finally { db.close() }
    repeat(2) {
        db = factory(target); db.open()
        try {
            val store = LockerStoreImpl(db)
            assertEquals(11, store.getAllLockers(RoomId(byteArrayOf(1)), LockerKeyspace(4)).single().version)
            val archives = store.archivedRatchets()
            if (version == 3) assertTrue(archives.isEmpty()) else {
                assertEquals(3, archives.size)
                val adopted = archives.single { it.scope.lockerRawValue.contentEquals(byteArrayOf(2)) }
                val legacyAdopted = archives.single { it.scope.lockerRawValue.contentEquals(byteArrayOf(3)) }
                assertNull(legacyAdopted.state!!.scope); assertEquals(0L, legacyAdopted.state.lockVersion)
                assertContentEquals(legacyAuthority.pending.privateKey, legacyAdopted.pending.privateKey)
                assertEquals(Long.MAX_VALUE, archives.single { it.scope.lockerRawValue.contentEquals(byteArrayOf(4)) }.state!!.lockVersion)
                assertEquals(3L, adopted.state!!.lockVersion)
                assertContentEquals(newer.pending.privateKey, adopted.pending.privateKey)
                assertContentEquals(newer.publicKey, adopted.publicKey)
                assertContentEquals(unknown, adopted.pending.request.unknownFields)
                val pending = store.pendingRatchets().single()
                assertContentEquals(newer.pending.privateKey, pending.privateKey)
                if (version == 4) assertNull(pending.expectation) // Historic V4 never persisted this metadata.
                else {
                    assertTrue(pending.expectation!!.sourceUncertaintyAcknowledged)
                    assertContentEquals(older.state.toByteArray(), pending.expectation.previous.toByteArray())
                }
            }
            db.transaction(definitions.map { it.storeName }.toSet(), TransactionMode.READ_ONLY) {
                assertContentEquals(ackBytes, getAll(ackDefinition.storeName).single() as ByteArray)
                assertContentEquals(cacheBytes, getAll(LockerStoreImplDefinitionV1.storeName).single() as ByteArray)
                if (version >= 4) {
                    val preserved = getAll(RatchetArchiveDefinitionV1.storeName).map { it as ByteArray }
                    assertEquals(4, preserved.size)
                    archiveBytes.forEach { bytes -> assertTrue(preserved.any { it.contentEquals(bytes) }) }
                    assertContentEquals(pendingBytes, getAll(RatchetJournalDefinitionV1.storeName).single() as ByteArray)
                    if (version == 5) assertContentEquals(expectationBytes,
                        getAll(RatchetExpectationDefinitionV2.storeName).single() as ByteArray)
                }
            }
        } finally { db.close() }
    }
}
