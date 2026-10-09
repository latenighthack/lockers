package com.latenighthack.lockers.connector.internal

import com.latenighthack.ktstore.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.yield
import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.canonical
import com.latenighthack.lockers.room.v1.fromByteArray
import com.latenighthack.lockers.room.v1.toByteArray

/** Latest committed private key per authority scope; it is not discarded on a volatile callback. */
data class ArchivedRatchet(val pending: PendingRatchet, val state: LockState?, val version: Long) {
    val room: RoomId get() = requireNotNull(pending.request.roomId)
    val publicKey: ByteArray get() = requireNotNull(pending.request.ratchet?.newPublicKey).rawValue
    val scope: LockScope get() = state?.scope ?: LockScope(kind = LockScopeKind.LOCK_SCOPE_LOCKER,
        keyspace = pending.request.lockerId!!.canonical().keyspace, lockerRawValue = pending.request.lockerId!!.rawValue)
}
private val ArchivedRatchet.roomKey: ByteArray get() = room.rawValue
private val ArchivedRatchet.scopeKey: ByteArray get() = when (scope.kind) {
    LockScopeKind.LOCK_SCOPE_ROOM -> LockScope(kind = scope.kind)
    LockScopeKind.LOCK_SCOPE_KEYSPACE -> LockScope(kind = scope.kind, keyspace = scope.keyspace ?: LockerKeyspace(0))
    else -> scope.copy(keyspace = scope.keyspace ?: LockerKeyspace(0))
}.toByteArray()
private fun encodeArchive(value: ArchivedRatchet) = encodeFrames(value.pending.request.toByteArray(), value.pending.privateKey, value.state?.toByteArray() ?: byteArrayOf(), longBytes(value.version))
private fun decodeArchive(bytes: ByteArray): ArchivedRatchet {
    val fields = decodeFrames(bytes); require(fields.size == 4)
    return ArchivedRatchet(PendingRatchet(com.latenighthack.lockers.room.v1.PostLockerChangeRequest.fromByteArray(fields[0]), fields[1]), fields[2].takeIf { it.isNotEmpty() }?.let { LockState.fromByteArray(it) }, bytesLong(fields[3]))
}
object RatchetArchiveDefinitionV1 : StoreDefinition<ArchivedRatchet>(StoreName("adopted_ratchets"), "ArchivedRatchet-frames-v1", ::decodeArchive, ::encodeArchive) {
    val room = bytesIndex(IndexName("room"), ArchivedRatchet::roomKey, "room-bytes-v1")
    val scope = bytesIndex(IndexName("scope"), ArchivedRatchet::scopeKey, "canonical-scope-protobuf-v1")
    val publicKey = bytesIndex(IndexName("public_key"), ArchivedRatchet::publicKey, "P256-public-key-v1")
    val roomScope = compositeIndex(IndexName("room_scope"), room, scope).also { primaryKey(it) }
    val roomPublicKey = compositeIndex(IndexName("room_public_key"), room, publicKey)
}
/** Original V1 payload bytes are retained; only the V2 index identity is normalized. */
internal data class ArchiveRecordV2(val bytes: ByteArray) {
    val archive: ArchivedRatchet get() = decodeArchive(bytes)
}
private val ArchiveRecordV2.roomKey: ByteArray get() = archive.roomKey
private val ArchiveRecordV2.scopeKey: ByteArray get() = archive.scope.let { scope ->
    require((scope.keyspace?.value ?: 0) >= 0) { "Invalid private archive keyspace" }
    require(scope.kind != LockScopeKind.LOCK_SCOPE_LOCKER || scope.lockerRawValue.isNotEmpty())
    when (scope.kind) {
        LockScopeKind.LOCK_SCOPE_ROOM -> LockScope(kind = scope.kind)
        LockScopeKind.LOCK_SCOPE_KEYSPACE -> LockScope(kind = scope.kind, keyspace = LockerKeyspace(scope.keyspace?.value ?: 0))
        LockScopeKind.LOCK_SCOPE_LOCKER -> LockScope(kind = scope.kind, keyspace = LockerKeyspace(scope.keyspace?.value ?: 0),
            lockerRawValue = scope.lockerRawValue.copyOf())
        else -> throw IllegalArgumentException("Invalid private archive scope")
    }
}.toByteArray()
private val ArchiveRecordV2.publicKey: ByteArray get() = archive.publicKey
internal object RatchetArchiveDefinitionV2 : StoreDefinition<ArchiveRecordV2>(
    StoreName("adopted_ratchets_v2"), "ArchivedRatchet-preserved-frames-v2",
    { ArchiveRecordV2(it.copyOf()) }, { it.bytes.copyOf() },
) {
    val room = bytesIndex(IndexName("room"), ArchiveRecordV2::roomKey, "room-bytes-v2")
    val scope = bytesIndex(IndexName("scope"), ArchiveRecordV2::scopeKey, "logical-known-scope-protobuf-v2")
    val publicKey = bytesIndex(IndexName("public_key"), ArchiveRecordV2::publicKey, "P256-public-key-v2")
    val roomScope = compositeIndex(IndexName("room_scope"), room, scope).also { primaryKey(it) }
    val roomPublicKey = compositeIndex(IndexName("room_public_key"), room, publicKey)
}
internal data class ArchiveMigrationProgress(val processed: Long, val total: Long, val complete: Boolean, val key: Int = 1)
internal object ArchiveMigrationDefinitionV2 : StoreDefinition<ArchiveMigrationProgress>(
    StoreName("ratchet_archive_migration_v2"), "ArchiveMigrationProgress-frames-v2",
    { bytes -> decodeFrames(bytes).let { fields ->
        require(fields.size == 3 && fields[2].size == 1 && fields[2][0] in 0..1)
        ArchiveMigrationProgress(bytesLong(fields[0]), bytesLong(fields[1]), fields[2][0] == 1.toByte()).also {
            require(it.processed in 0..it.total && it.total <= 10_000 && it.complete == (it.processed == it.total))
        }
    } },
    { encodeFrames(longBytes(it.processed), longBytes(it.total), byteArrayOf(if (it.complete) 1 else 0)) },
) {
    val key = integerIndex(IndexName("key"), ArchiveMigrationProgress::key).also { primaryKey(it) }
}
private class ArchiveProgress(database: Database) : Store<ArchiveMigrationProgress>(database, ArchiveMigrationDefinitionV2) {
    suspend fun current() = get(ArchiveMigrationDefinitionV2.key.eq(1))
    suspend fun put(value: ArchiveMigrationProgress) = save(value)
}
internal class RatchetArchive(private val database: Database) : Store<ArchiveRecordV2>(database, RatchetArchiveDefinitionV2) {
    private val progress = ArchiveProgress(database)
    private fun detached(record: Any) = ArchiveRecordV2(when (record) {
        is ArchiveRecordV2 -> record.bytes.copyOf()
        is ArchivedRatchet -> encodeArchive(record)
        else -> (record as ByteArray).also {
            require(it.size <= 8 * 1024 * 1024 + 64 * 1024) { "Private archive row exceeds migration bound" }
        }.copyOf()
    })
    private suspend fun validated(record: ArchiveRecordV2): ArchivedRatchet {
        require(record.bytes.size <= 8 * 1024 * 1024 + 64 * 1024) { "Private archive row exceeds migration bound" }
        val value = record.archive
        require(value.version >= 0 && value.pending.privateKey.size == 32 && value.publicKey.size == 33)
        require(value.room.rawValue.isNotEmpty()) // Legacy authority archives may predate write IDs.
        record.scopeKey // Reject unknown scope kinds before deriving a key.
        value.state?.let { state ->
            require(state.locked && ((state.scope == null && state.lockVersion == 0L) ||
                (state.scope != null && state.lockVersion >= 1L))) { "Contradictory private archive authority metadata" }
            require(state.publicKey?.rawValue.contentEquals(value.publicKey)) { "Private archive authority mismatch" }
        }
        val key = requireNotNull(Secp256r1KeyPair.fromPrivateKey(value.pending.privateKey)) { "Invalid archived private key" }
        require(key.publicKey.encode().contentEquals(value.publicKey)) { "Archived private/public key mismatch" }
        return value
    }
    private fun identity(record: ArchiveRecordV2) = RatchetArchiveDefinitionV2.roomScope.eq(listOf(
        BoundStoreKey.SerializedKey(RatchetArchiveDefinitionV2.room.name.value, record.roomKey),
        BoundStoreKey.SerializedKey(RatchetArchiveDefinitionV2.scope.name.value, record.scopeKey)))
    /** Local database work only; callers validate cryptography before entering the owner transaction. */
    private suspend fun merge(record: ArchiveRecordV2) {
        val existing = get(identity(record))
        if (existing != null) {
            val current = existing.archive
            val candidate = record.archive
            val currentEpoch = current.state?.lockVersion ?: 0L
            val candidateEpoch = candidate.state?.lockVersion ?: 0L
            if (candidateEpoch < currentEpoch) return
            if (candidateEpoch == currentEpoch) {
                if (candidateEpoch > 0 || candidate.version == current.version) {
                    require(current.publicKey.contentEquals(candidate.publicKey) &&
                        current.pending.privateKey.contentEquals(candidate.pending.privateKey)) {
                        "Conflicting private archive authority epoch"
                    }
                    return
                }
                // Epoch-zero legacy fallback is one locker; source versions are comparable there only.
                if (candidate.version < current.version) return
            }
        } else if (database.count(RatchetArchiveDefinitionV2.storeName,
                IndexedQuery(RatchetArchiveDefinitionV2.room.key, 1)) >= 10_000) {
            throw com.latenighthack.lockers.connector.ConnectorRetentionExceededException("Ratchet archive admission limit exceeded")
        }
        save(record)
    }
    /** V6 never writes V1. Its immutable ordered rank is a durable cursor across cancellation/reopen. */
    private suspend fun prepareCurrent() {
        prepare(); progress.prepare()
        val initial = database.transaction("connector-ratchet") {
            progress.current() ?: run {
                val total = database.count(RatchetArchiveDefinitionV1.storeName,
                    IndexedQuery(RatchetArchiveDefinitionV1.room.key, 1))
                if (total > 10_000) throw com.latenighthack.lockers.connector.ConnectorRetentionExceededException(
                    "Historical ratchet archive migration admission limit exceeded")
                ArchiveMigrationProgress(0, total, total == 0L).also { progress.put(it) }
            }
        }
        if (initial.complete) return
        var after: LocalContinuation? = null
        var rank = 0L
        do {
            currentCoroutineContext().ensureActive()
            val observed = database.transaction("connector-ratchet") { requireNotNull(progress.current()) }
            if (observed.complete) return
            val page = database.query(RatchetArchiveDefinitionV1.storeName,
                IndexedQuery(RatchetArchiveDefinitionV1.room.key, 4, after = after))
            val end = rank + page.records.size
            require(end <= observed.total && (page.continuation != null || end == observed.total)) {
                "Historical archive changed during migration"
            }
            if (end > observed.processed) {
                val candidates = page.records.drop((observed.processed - rank).coerceAtLeast(0).toInt()).map(::detached)
                candidates.forEach { validated(it) } // Crypto awaits are outside the database owner.
                database.transaction("connector-ratchet") {
                    val current = requireNotNull(progress.current())
                    if (current == observed) {
                        candidates.forEach { merge(it) }
                        progress.put(ArchiveMigrationProgress(end, observed.total, end == observed.total))
                    }
                }
            }
            rank = end; after = page.continuation
            yield() // Never retain the database owner across coroutine scheduling or crypto work.
        } while (after != null)
        check(database.transaction("connector-ratchet") { requireNotNull(progress.current()).complete }) {
            "Archive migration did not complete"
        }
    }
    fun entries(): Flow<ArchivedRatchet> = flow {
        prepareCurrent(); var after: LocalContinuation? = null
        do {
            val page = database.query(RatchetArchiveDefinitionV2.storeName,
                IndexedQuery(RatchetArchiveDefinitionV2.room.key, 1, after = after))
            page.records.forEach { emit(validated(detached(it))) }
            after = page.continuation
        } while (after != null)
    }
    suspend fun archives(): List<ArchivedRatchet> = entries().toList()
    suspend fun hasRoom(room: RoomId): Boolean {
        val roomBytes = room.rawValue.copyOf()
        prepareCurrent(); return get(RatchetArchiveDefinitionV2.room.eq(roomBytes)) != null
    }
    suspend fun put(value: ArchivedRatchet) {
        val record = ArchiveRecordV2(encodeArchive(value))
        prepareCurrent(); validated(record)
        database.transaction("connector-ratchet") { merge(record) }
    }
    suspend fun matching(room: RoomId, publicKey: ByteArray): ArchivedRatchet? {
        val roomBytes = room.rawValue.copyOf(); val publicKeyBytes = publicKey.copyOf()
        prepareCurrent()
        val record = get(RatchetArchiveDefinitionV2.roomPublicKey.eq(listOf(
            BoundStoreKey.SerializedKey(RatchetArchiveDefinitionV2.room.name.value, roomBytes),
            BoundStoreKey.SerializedKey(RatchetArchiveDefinitionV2.publicKey.name.value, publicKeyBytes)))) ?: return null
        return validated(detached(record))
    }
    suspend fun remove(value: ArchivedRatchet) {
        val record = ArchiveRecordV2(encodeArchive(value))
        prepareCurrent()
        database.transaction("connector-ratchet") {
            val current = get(identity(record)) ?: return@transaction
            if (encodeArchive(current.archive).contentEquals(record.bytes)) delete(identity(record))
        }
    }
}
