package com.latenighthack.lockers.connector.internal

import com.latenighthack.ktstore.*
import kotlinx.coroutines.flow.*
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
internal class RatchetArchive(private val database: Database) : Store<ArchivedRatchet>(database, RatchetArchiveDefinitionV1) {
    fun entries(): Flow<ArchivedRatchet> = flow {
        prepare(); var after: LocalContinuation? = null
        do {
            val page = database.query(RatchetArchiveDefinitionV1.storeName, IndexedQuery(RatchetArchiveDefinitionV1.room.key, 1, after = after))
            page.records.forEach { emit(decodeArchive(if (it is ArchivedRatchet) encodeArchive(it) else it as ByteArray)) }
            after = page.continuation
        } while (after != null)
    }
    suspend fun archives(): List<ArchivedRatchet> = entries().toList()
    suspend fun hasRoom(room: RoomId): Boolean { prepare(); return get(RatchetArchiveDefinitionV1.room.eq(room.rawValue)) != null }
    suspend fun put(value: ArchivedRatchet) {
        prepare(); database.transaction("connector-ratchet") {
            val identity = RatchetArchiveDefinitionV1.roomScope.eq(listOf(BoundStoreKey.SerializedKey(RatchetArchiveDefinitionV1.room.name.value, value.roomKey), BoundStoreKey.SerializedKey(RatchetArchiveDefinitionV1.scope.name.value, value.scopeKey)))
            val existing = get(identity)
            if (existing != null) {
                val currentEpoch = existing.state?.lockVersion ?: 0L
                val candidateEpoch = value.state?.lockVersion ?: 0L
                if (candidateEpoch < currentEpoch) return@transaction
                if (candidateEpoch == currentEpoch) {
                    if (candidateEpoch > 0) {
                        require(existing.publicKey.contentEquals(value.publicKey)) { "Conflicting private archive authority epoch" }
                        return@transaction
                    }
                    // Legacy fallback scope is a single locker: source versions are comparable there.
                    if (value.version <= existing.version) return@transaction
                }
            }
            if (existing == null && database.count(RatchetArchiveDefinitionV1.storeName, IndexedQuery(RatchetArchiveDefinitionV1.room.key, 1)) >= 10_000)
                throw com.latenighthack.lockers.connector.ConnectorRetentionExceededException("Ratchet archive admission limit exceeded")
            save(decodeArchive(encodeArchive(value)))
        }
    }
    suspend fun matching(room: RoomId, publicKey: ByteArray): ArchivedRatchet? {
        prepare(); return get(RatchetArchiveDefinitionV1.roomPublicKey.eq(listOf(
            BoundStoreKey.SerializedKey(RatchetArchiveDefinitionV1.room.name.value, room.rawValue),
            BoundStoreKey.SerializedKey(RatchetArchiveDefinitionV1.publicKey.name.value, publicKey))))
    }
    suspend fun remove(value: ArchivedRatchet) {
        prepare(); database.transaction("connector-ratchet") {
            val identity = RatchetArchiveDefinitionV1.roomScope.eq(listOf(
                BoundStoreKey.SerializedKey(RatchetArchiveDefinitionV1.room.name.value, value.roomKey),
                BoundStoreKey.SerializedKey(RatchetArchiveDefinitionV1.scope.name.value, value.scopeKey)))
            val current = get(identity) ?: return@transaction
            if (encodeArchive(current).contentEquals(encodeArchive(value))) delete(identity)
        }
    }
}
