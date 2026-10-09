package com.latenighthack.lockers.server.services.room.v1

import kotlinx.coroutines.ensureActive

import com.latenighthack.ktstore.*
import com.latenighthack.ktcrypto.SHA256
import com.latenighthack.ktcrypto.digest
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.room.v1.*
import com.latenighthack.lockers.server.*
import com.latenighthack.ktbuf.net.RpcResponseException
import com.latenighthack.ktbuf.proto.Codes
import java.io.*
import java.security.SecureRandom

data class SnapshotRecord(val token: ByteArray, val index: Int, val binding: ByteArray,
    val room: ByteArray, val session: ByteArray, val expiry: Long, val bytes: Long, val encoded: ByteArray) {
    val identity: ByteArray get() = token + byteArrayOf((index ushr 24).toByte(), (index ushr 16).toByte(), (index ushr 8).toByte(), index.toByte())
    val lease: Boolean get() = index == -1
    val leaseExpiry: Long? get() = if (lease) expiry else null
}
private fun snapshotEncode(row: SnapshotRecord): ByteArray = ByteArrayOutputStream().use { buffer ->
    DataOutputStream(buffer).use { out ->
        fun bytes(value: ByteArray) { out.writeInt(value.size); out.write(value) }
        bytes(row.token); out.writeInt(row.index); bytes(row.binding); bytes(row.room); bytes(row.session)
        out.writeLong(row.expiry); out.writeLong(row.bytes); bytes(row.encoded)
    }; buffer.toByteArray()
}
private fun snapshotDecode(raw: ByteArray): SnapshotRecord = DataInputStream(ByteArrayInputStream(raw)).use { input ->
    fun bytes(max: Int): ByteArray { val size = input.readInt(); require(size in 0..max && size <= input.available()); return ByteArray(size).also(input::readFully) }
    val result = SnapshotRecord(bytes(32), input.readInt(), bytes(32), bytes(128), bytes(128), input.readLong(), input.readLong(), bytes(ProtocolValidation.MAX_ENVELOPE_BYTES))
    require(result.token.size == 32 && result.binding.size == 32 && input.available() == 0)
    result
}
internal object SnapshotDefinitionV2 : StoreDefinition<SnapshotRecord>(StoreName("snapshot_pages_v2"), "bounded-snapshot-page-v2", ::snapshotDecode, ::snapshotEncode) {
    val identity = bytesIndex(IndexName("identity"), SnapshotRecord::identity, "token-page-v2").also { primaryKey(it) }
    val lease = booleanIndex(IndexName("lease"), SnapshotRecord::lease)
    val session = bytesIndex(IndexName("session"), SnapshotRecord::session, "raw-session-v2")
    val leaseExpiry = nullableMappedIndex(IndexName("leaseExpiry"), SnapshotRecord::leaseExpiry, object : StorageCodec<Long, ByteArray> {
        override fun key(name: IndexName) = StoreKey.SerializedKey(name.value)
        override fun encode(value: Long): ByteArray { val v = value xor Long.MIN_VALUE; return ByteArray(8) { (v ushr (56 - it * 8)).toByte() } }
    }, "nullable-sortable-long-v2")
}

/** Immutable leased pages are durable and work when the next request reaches another replica. */
class SnapshotStore(private val database: Database, private val limits: ServerResourceLimits,
    private val clock: () -> Long = System::currentTimeMillis) : Store<SnapshotRecord>(database, SnapshotDefinitionV2) {
    private val random = SecureRandom()
    private fun exhausted(message: String): Nothing = throw RpcResponseException("", "RPC", Codes.RESOURCE_EXHAUSTED, message)
    private suspend fun binding(operation: Int, room: RoomId, session: SessionId?, spaces: Set<Long>, pageSize: Int): ByteArray =
        SHA256.digest(ByteArrayOutputStream().use { buffer ->
            DataOutputStream(buffer).use { out ->
                out.writeInt(operation); out.writeInt(pageSize); out.writeInt(room.rawValue.size); out.write(room.rawValue)
                val sid = session?.rawValue ?: byteArrayOf(); out.writeInt(sid.size); out.write(sid)
                out.writeInt(spaces.size); spaces.sorted().forEach(out::writeLong)
            }; buffer.toByteArray()
        })
    fun validate(pageSize: Int, token: ByteArray) {
        if (pageSize !in 0..64 || (token.isNotEmpty() && (pageSize == 0 || token.size != 36))) invalidArgument("Invalid snapshot paging request")
    }
    suspend fun next(operation: Int, room: RoomId, session: SessionId?, spaces: Set<Long>, pageSize: Int, token: ByteArray): GetAllLockersResponse = database.transaction("snapshot-admission") {
        validate(pageSize, token)
        val row = get(SnapshotDefinitionV2.identity.eq(token)) ?: invalidArgument("Unknown snapshot page")
        if (row.index < 1 || row.expiry <= clock() || !row.binding.contentEquals(binding(operation, room, session, spaces, pageSize)))
            invalidArgument("Expired or foreign snapshot page")
        GetAllLockersResponse.fromByteArray(row.encoded)
    }
    suspend fun create(operation: Int, room: RoomId, session: SessionId?, spaces: Set<Long>, pageSize: Int,
        sequence: Long, lockers: List<IdentifiedLocker>): GetAllLockersResponse = database.transaction("snapshot-admission") {
        validate(pageSize, byteArrayOf())
        var total = 0L
        val chunks = mutableListOf<List<IdentifiedLocker>>(); var chunk = mutableListOf<IdentifiedLocker>(); var chunkBytes = 1024
        for (locker in lockers) {
            val size = locker.toByteArray().size + 16; total += size
            if (size + 1024 > ProtocolValidation.MAX_ENVELOPE_BYTES || total > limits.maxSnapshotBytes || lockers.size > limits.maxSnapshotLockers)
                protocolCapacityExceeded("Snapshot exceeds capture capacity")
            if (pageSize > 0 && (chunk.size == pageSize || chunkBytes + size > ProtocolValidation.MAX_ENVELOPE_BYTES)) {
                chunks.add(chunk.toList()); chunk = mutableListOf(); chunkBytes = 1024
            }
            chunk.add(locker); chunkBytes += size
        }
        if (pageSize == 0) {
            if (lockers.size > limits.maxLegacySnapshotLockers || chunkBytes > ProtocolValidation.MAX_ENVELOPE_BYTES)
                protocolCapacityExceeded("Complete legacy snapshot exceeds envelope; select paging")
            return@transaction GetAllLockersResponse(lockers = lockers, roomSequence = sequence)
        }
        if (chunk.isNotEmpty() || chunks.isEmpty()) chunks.add(chunk.toList())
        if (chunks.size == 1) return@transaction GetAllLockersResponse(lockers = chunks.single(), roomSequence = sequence)
        val expired = database.query(SnapshotDefinitionV2.storeName, SnapshotDefinitionV2.leaseExpiry.query(4, upper = clock())).records
        for (raw in expired) {
            val row = when (raw) { is SnapshotRecord -> raw; is ByteArray -> SnapshotDefinitionV2.decode(raw); else -> error("Invalid snapshot row") }
            val upper = row.token.copyOf().let { bytes ->
                val last = bytes.indexOfLast { (it.toInt() and 255) != 255 }
                check(last >= 0); bytes[last] = (bytes[last] + 1).toByte(); bytes.copyOf(last + 1)
            }
            database.deleteBatch(SnapshotDefinitionV2.storeName, SnapshotDefinitionV2.identity.query(limits.maxSnapshotLockers + 1,
                lower = row.token, upper = upper, upperInclusive = false))
        }
        val leases = database.query(SnapshotDefinitionV2.storeName, SnapshotDefinitionV2.lease.query(limits.maxSnapshotLeases + 1,
            lower = true, upper = true)).records.map { when (it) { is SnapshotRecord -> it; is ByteArray -> SnapshotDefinitionV2.decode(it); else -> error("Invalid snapshot row") } }
        if (leases.size >= limits.maxSnapshotLeases || leases.count { it.room.contentEquals(room.rawValue) } >= limits.maxSnapshotLeasesPerRoom ||
            leases.sumOf { it.bytes } + total > limits.maxSnapshotRetainedBytes) exhausted("Snapshot lease capacity exhausted")
        val token = ByteArray(32).also(random::nextBytes); val bind = binding(operation, room, session, spaces, pageSize); val expiry = clock() + limits.snapshotLeaseMillis
        fun pageToken(index: Int) = token + byteArrayOf((index ushr 24).toByte(), (index ushr 16).toByte(), (index ushr 8).toByte(), index.toByte())
        val responses = chunks.mapIndexed { index, rows -> GetAllLockersResponse(lockers = rows, roomSequence = sequence,
            nextPageToken = if (index + 1 < chunks.size) pageToken(index + 1) else byteArrayOf()) }
        saveAll(listOf(SnapshotRecord(token, -1, bind, room.rawValue, session?.rawValue ?: byteArrayOf(), expiry, total, byteArrayOf())) +
            responses.drop(1).mapIndexed { index, response -> SnapshotRecord(token, index + 1, bind, room.rawValue, session?.rawValue ?: byteArrayOf(), expiry, 0, response.toByteArray()) })
        responses.first()
    }
    suspend fun deleteAllForSession(session: ByteArray) = database.transaction("snapshot-admission") {
        while (database.deleteBatch(SnapshotDefinitionV2.storeName,
                SnapshotDefinitionV2.session.query(256, lower = session, upper = session)) > 0) {
            kotlinx.coroutines.currentCoroutineContext().ensureActive()
        }
    }
}
