package com.latenighthack.lockers.connector

import com.latenighthack.lockers.observability.*

import com.diamondedge.logging.KmLog
import com.diamondedge.logging.logging
import com.latenighthack.ktbuf.bytes.toBase64String
import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktbuf.net.RpcResponseException
import com.latenighthack.ktbuf.rpc.RetryLimitExceeded
import com.latenighthack.ktbuf.rpc.repeatWithBackoff
import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.LockerEnvelope
import com.latenighthack.lockers.common.SessionSigning
import com.latenighthack.lockers.common.LockerSigning
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.internal.ArchivedRatchet
import com.latenighthack.lockers.connector.internal.PendingRatchet
import com.latenighthack.lockers.connector.internal.LockerStore
import com.latenighthack.lockers.connector.internal.ShardedRoomServiceRpc
import com.latenighthack.lockers.connector.storage.v1.fromByteArray
import com.latenighthack.lockers.connector.storage.v1.StoredLocker
import com.latenighthack.lockers.room.v1.*
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import kotlin.reflect.KFunction1

/**
 * The application bytes of a locker, regardless of envelope. Open lockers carry them
 * directly; signed/sealed lockers carry them in the (cleartext) enclosure. Callers at
 * the high level never see the envelope — they only ever get these bytes.
 */
internal fun Locker.plaintextPayload(): ByteArray =
    LockerEnvelope.payload(this)

private fun IdentifiedLocker.toUpdate(roomId: RoomId, roomSequence: Long = 0L) =
    LockerClient.LockerUpdate(roomId.canonical(), lockerId!!.canonical(), version, locker?.plaintextPayload() ?: byteArrayOf(), deleted = locker == null, roomSequence = roomSequence)

private fun StoredLocker.toIdentifiedLocker(): IdentifiedLocker {
    val storedVersion = version
    val storedLockerId = LockerId(lockerIdRawValue.copyOf(), LockerKeyspace { value = lockerKeyspace })
    val storedPayload = lockerPayload.copyOf()

    return IdentifiedLocker {
        lockerId = storedLockerId
        locker = Locker { open { encodedPayload = storedPayload } }
        version = storedVersion
    }
}

/** Identity is raw bytes plus numeric keyspace; unknown wrapper fields are not authority. */
internal fun RoomId.canonical() = RoomId(rawValue.copyOf())
internal fun SessionId.canonical() = SessionId(rawValue.copyOf())
internal fun LockerKeyspace.canonical() = LockerKeyspace(value)
internal fun LockerId.canonical() = LockerId(rawValue.copyOf(), LockerKeyspace(keyspace?.value ?: 0L))
internal fun LockScope.canonical() = LockScope(kind, keyspace?.canonical(), lockerRawValue.copyOf())

private fun LockerId.keyspaceOrDefault() = LockerKeyspace(keyspace?.value ?: 0L)

private fun LockerClient.LockerUpdate.toStored() = StoredLocker {
    roomIdRawValue = roomId.rawValue.copyOf()
    lockerIdRawValue = lockerId.rawValue.copyOf()
    lockerKeyspace = lockerId.keyspace?.value ?: 0L
    lockerPayload = payload.copyOf()
    version = this@toStored.version
    deleted = this@toStored.deleted
    roomSequence = this@toStored.roomSequence
}

/**
 * Supplies the signing keys for locked lockers. This is the client's sole opt-in to
 * locking: return a keypair for a locker and every write to it is automatically
 * wrapped in a signed envelope; return null and writes stay open. The source owns the
 * scope→key mapping (a per-locker, per-keyspace, or per-room key can back many
 * lockers) so the low-level protocol never needs to know the scope for a plain write.
 * Enabling ratchets writes private recovery keys to the connector database. Use trusted confidential
 * storage and controlled backups; latest-per-scope keys remain until authoritative replacement.
 */
interface LockKeySource {
    suspend fun writeKeyFor(roomId: RoomId, lockerId: LockerId): Secp256r1KeyPair?

    /** Persist the adopted key before returning. The connector confirms lookup and retains a durable recovery copy. */
    suspend fun onRatcheted(roomId: RoomId, lockerId: LockerId, newKeyPair: Secp256r1KeyPair) {}
}

/**
 * A typed view of a locker change. [Present] carries the decoded value; [Deleted]
 * signals the locker was removed (locally or remotely) so watchers can drop it.
 */
sealed interface TypedLockerUpdate<out V> {
    val roomId: RoomId
    val lockerId: LockerId

    data class Present<V>(
        override val roomId: RoomId,
        override val lockerId: LockerId,
        val value: V,
    ) : TypedLockerUpdate<V>

    data class Deleted(
        override val roomId: RoomId,
        override val lockerId: LockerId,
    ) : TypedLockerUpdate<Nothing>
}

class TypedLockerClient<ValueType>(
    private val lockerClient: LockerClient,
    keyspace: LockerKeyspace,
    private val writer: KFunction1<ValueType, ByteArray>,
    private val reader: KFunction1<ByteArray, ValueType>
) {
    private val keyspace = keyspace.canonical()
    private fun LockerId.scoped(): LockerId {
        val existing = keyspace
        require(existing == null || existing.value == this@TypedLockerClient.keyspace.value) {
            "LockerId keyspace ${existing?.value} does not match this client's keyspace ${this@TypedLockerClient.keyspace.value}"
        }
        return LockerId(rawValue.copyOf(), this@TypedLockerClient.keyspace.canonical())
    }

    suspend fun getLocker(roomId: RoomId, lockerId: LockerId, revalidate: Boolean = true): ValueType? {
        val fetched = lockerClient.getLocker(roomId, lockerId.scoped(), revalidate)
        return fetched?.locker?.let { reader(it.plaintextPayload()) }
    }

    suspend fun getAllLockers(roomId: RoomId, revalidate: Boolean = true): Map<LockerId, ValueType> =
        lockerClient.getAllLockers(roomId, keyspace, revalidate)
            .associate { it.lockerId!! to reader(it.locker?.plaintextPayload() ?: byteArrayOf()) }

    suspend fun deleteLocker(
        roomId: RoomId,
        lockerId: LockerId,
        notificationBuilder: NotificationBuilder.() -> Unit = {}
    ) {
        lockerClient.deleteLocker(roomId, lockerId.scoped(), notificationBuilder)
    }

    suspend fun updateLocker(
        roomId: RoomId,
        lockerId: LockerId,
        notificationBuilder: NotificationBuilder.(Locker?) -> Unit = {},
        ratchet: Boolean = false,
        builder: (ValueType) -> ValueType
    ): ValueType? {
        val scopedId = lockerId.scoped()
        val updated = lockerClient.updateLocker(roomId, scopedId, notificationBuilder, ratchet) { existing ->
            writer(builder(reader(existing)))
        }

        return updated?.let { reader(it.plaintextPayload()) }
    }

    suspend fun lockLocker(
        roomId: RoomId,
        scope: LockScope,
        keyPair: Secp256r1KeyPair,
        parentKeyPair: Secp256r1KeyPair? = null,
        parentLockVersion: Long = 0L
    ) = lockerClient.lockLocker(roomId, scope, keyPair, parentKeyPair, parentLockVersion)

    suspend fun unlockLocker(
        roomId: RoomId,
        scope: LockScope,
        keyPair: Secp256r1KeyPair,
        parentLockVersion: Long
    ) = lockerClient.unlockLocker(roomId, scope, keyPair, parentLockVersion)

    fun watchAll(roomId: RoomId, includeHistory: Boolean = true): Flow<Map<LockerId, ValueType>> =
        watchAllIn(roomId, keyspace, includeHistory)

    fun watchAll(roomId: RoomId, keyspace: LockerKeyspace, includeHistory: Boolean = true): Flow<Map<LockerId, ValueType>> =
        watchAllIn(roomId, keyspace, includeHistory)

    // The stored keyspace is one getAllLockers read, emitted as ONE snapshot map that live
    // updates then fold onto. (Hydration used to replay each cached locker as a separate update
    // event through the fold, so every subscriber observed the map re-assemble {} -> {a} -> {a,b}.)
    // A failed subscribe/hydrate must not tear down the watcher; live updates still flow.
    // No ACK wait: offline, cached lockers must still hydrate (reconnect reconciles the sub).
    // The shared coordinator installs live collection before hydration and merges versions.
    private fun watchAllIn(roomId: RoomId, keyspace: LockerKeyspace, includeHistory: Boolean): Flow<Map<LockerId, ValueType>> {
        val roomId = roomId.canonical(); val keyspace = keyspace.canonical()
        return if (includeHistory) lockerClient.watchSnapshot(roomId, keyspace).map { items ->
            items.associate { it.lockerId!! to reader(it.locker!!.plaintextPayload()) }
        } else allUpdates.filter { it.lockerId.keyspace == keyspace && it.roomId == roomId }
            .onStart { lockerClient.subscribeToRoom(roomId, false) }
            .runningFold(emptyMap()) { acc, value -> acc.applyUpdate(value) }

    }

    fun watch(roomId: RoomId, lockerId: LockerId, includeHistory: Boolean = true): Flow<TypedLockerUpdate<ValueType>> {
        val roomId = roomId.canonical()
        val scopedId = lockerId.scoped()
        if (!includeHistory) return allUpdates.filter { it.roomId == roomId && it.lockerId == scopedId }
            .onStart { lockerClient.subscribeToRoom(roomId, false) }
        return lockerClient.watchSnapshot(roomId, keyspace).map { items ->
            items.firstOrNull { it.lockerId == scopedId }?.toTyped(roomId)
                ?: TypedLockerUpdate.Deleted(roomId, scopedId)
        }.distinctUntilChanged()
    }

    val allUpdates: Flow<TypedLockerUpdate<ValueType>>
        get() {
            return lockerClient
                .changes
                .filter { it.lockerId.keyspace == keyspace }
                .map { it.toTyped() }
        }

    val notifications: Flow<IncomingNotification>
        get() {
            return lockerClient
                .notifications
                .filter { it.lockerId.keyspace == keyspace }
        }

    suspend fun subscribeToRoom(roomId: RoomId, waitForSubscription: Boolean = true) {
        lockerClient.subscribeToRoom(roomId, waitForSubscription)
    }

    suspend fun unsubscribeFromRoom(roomId: RoomId) {
        lockerClient.unsubscribeFromRoom(roomId)
    }

    private fun IdentifiedLocker.toTyped(roomId: RoomId): TypedLockerUpdate<ValueType> =
        TypedLockerUpdate.Present(roomId, lockerId!!, reader(locker?.plaintextPayload() ?: byteArrayOf()))

    private fun LockerClient.LockerUpdate.toTyped(): TypedLockerUpdate<ValueType> =
        if (deleted) {
            TypedLockerUpdate.Deleted(roomId, lockerId)
        } else {
            TypedLockerUpdate.Present(roomId, lockerId, reader(payload))
        }

    private fun Map<LockerId, ValueType>.applyUpdate(update: TypedLockerUpdate<ValueType>): Map<LockerId, ValueType> =
        when (update) {
            is TypedLockerUpdate.Deleted -> this - update.lockerId
            is TypedLockerUpdate.Present -> this + (update.lockerId to update.value)
        }
}

class IncomingNotification(
    val roomId: RoomId,
    val lockerId: LockerId,
    val payload: ByteArray
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is IncomingNotification) return false

        return roomId == other.roomId &&
            lockerId == other.lockerId &&
            payload.contentEquals(other.payload)
    }

    override fun hashCode(): Int {
        var result = roomId.hashCode()
        result = 31 * result + lockerId.hashCode()
        result = 31 * result + payload.contentHashCode()
        return result
    }
}

private const val WRITE_RETRY_LIMIT = 8

/**
 * A [LockerWriteException] is terminal (a bad/absent signing key, or an unauthorized
 * write) and must not be retried — otherwise the write would burn the whole retry
 * budget before surfacing. RPC errors keep their usual transient/terminal split.
 */
private val WRITE_EXCEPTION_HANDLER: (Throwable) -> Boolean = { e ->
    if (e is CancellationException) throw e
    writeRetryLog.debug { "locker write attempt failed (${e::class.simpleName}: ${e.message})\n${e.stackTraceToString()}" }
    when (e) {
        is LockerWriteException -> false
        is RpcResponseException -> e.retriable()
        else -> true
    }
}

private val writeRetryLog = com.diamondedge.logging.logging("LockerWriteRetry")

/**
 * Thrown when a locker write cannot be completed — the server reported a terminal
 * error or the optimistic-concurrency retry budget ([WRITE_RETRY_LIMIT]) was
 * exhausted. Distinguishes a failed write from a merely slow one; without it a
 * permanent server error would retry forever.
 */
open class LockerWriteException(message: String, cause: Throwable? = null) : Exception(message, cause)

/** Delete is a compare-and-set operation; retrying a conflict requires a new explicit caller decision. */
class LockerDeleteConflictException(
    val expectedVersion: Long, val actualVersion: Long, val mayHaveCommitted: Boolean, cause: Throwable? = null,
) : LockerWriteException("Delete expected version $expectedVersion but found $actualVersion" +
    if (mayHaveCommitted) "; an earlier legacy attempt may have committed" else "", cause)

/** The source and authority committed successfully; only derived work is incomplete. */
open class LockerSourceCommittedException(
    val version: Long, val agentPending: Boolean,
    message: String = "Source committed at version $version; agent ${if (agentPending) "pending" else "failed"}", cause: Throwable? = null,
    roomId: RoomId? = null, writeRequestId: ByteArray = byteArrayOf(), sourceVersions: List<WriteSourceVersion> = emptyList(),
    val agentIndeterminate: Boolean = false,
) : LockerWriteException(message, cause) {
    private val roomBytes = roomId?.rawValue?.copyOf()
    private val requestBytes = writeRequestId.copyOf()
    private val sources = sourceVersions.map { it.copy(lockerId = it.lockerId?.let { id -> id.copy(rawValue = id.rawValue.copyOf()) }) }
    val roomId: RoomId? get() = roomBytes?.let { RoomId(it.copyOf()) }
    val writeRequestId: ByteArray get() = requestBytes.copyOf()
    val sourceVersions: List<WriteSourceVersion> get() = sources.map { it.copy(lockerId = it.lockerId?.let { id -> id.copy(rawValue = id.rawValue.copyOf()) }) }
}

class RatchetAdoptionPendingException(version: Long, cause: Throwable? = null, roomId: RoomId? = null, writeRequestId: ByteArray = byteArrayOf(), sourceVersions: List<WriteSourceVersion> = emptyList()) :
    LockerSourceCommittedException(version, false, "Source committed at version $version; signing key adoption is pending", cause, roomId, writeRequestId, sourceVersions)

class LockerClient(
    rpcClient: RpcClient,
    private val stream: Stream,
    private val lockerStore: LockerStore,
    private val lockKeySource: LockKeySource? = null,
    codecs: NotificationCodecs = NotificationCodecs.identity(),
    internal val log: KmLog = logging(),
    private val telemetry: LockersTelemetry = LockersTelemetry.NONE,
    coroutineContext: kotlin.coroutines.CoroutineContext = Dispatchers.Default,
    private val broadcastCodecs: BroadcastCodecs = BroadcastCodecs.identity(),
) {
    private val codecs = codecs.withTelemetry(telemetry)
    private val processingJob = SupervisorJob(coroutineContext[Job])
    private val processingScope = CoroutineScope(coroutineContext + processingJob)
    private val started = MutableStateFlow(false)
    private val sync = LockerSyncCoordinator(processingScope, telemetry)
    private val ratchetAdoption = Mutex()
    private val acceptance = Mutex()
    private class AcceptanceContext(val client: LockerClient) : kotlin.coroutines.AbstractCoroutineContextElement(Key) {
        companion object Key : kotlin.coroutines.CoroutineContext.Key<AcceptanceContext>
    }
    private suspend fun <T> withAcceptance(action: suspend () -> T): T =
        if (currentCoroutineContext()[AcceptanceContext]?.client === this) action()
        else acceptance.withLock { withContext(AcceptanceContext(this)) { action() } }
    private class SnapshotWatch(val revision: MutableStateFlow<Long> = MutableStateFlow(0), var users: Int = 0)
    private val watched = mutableMapOf<Pair<RoomId, LockerKeyspace>, SnapshotWatch>()
    private val watchMutex = Mutex()

    class LockerUpdate(
        val roomId: RoomId,
        val lockerId: LockerId,
        val version: Long,
        val payload: ByteArray,
        val deleted: Boolean = false,
        val roomSequence: Long = 0L,
    ) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is LockerUpdate) return false

            return roomId == other.roomId &&
                lockerId == other.lockerId &&
                version == other.version &&
                deleted == other.deleted &&
                payload.contentEquals(other.payload)
        }

        override fun hashCode(): Int {
            var result = roomId.hashCode()
            result = 31 * result + lockerId.hashCode()
            result = 31 * result + version.hashCode()
            result = 31 * result + deleted.hashCode()
            result = 31 * result + payload.contentHashCode()
            return result
        }
    }

    private fun com.latenighthack.lockers.connector.internal.ConnectorJournalEntry.toChange(): LockerUpdate {
        val stored = StoredLocker.fromByteArray(payload)
        return LockerUpdate(RoomId(stored.roomIdRawValue), LockerId(stored.lockerIdRawValue, LockerKeyspace(stored.lockerKeyspace)), stored.version, stored.lockerPayload, stored.deleted, stored.roomSequence)
    }
    /** Recoverable changes; persist the cursor only after processing its change. */
    /** Cold parent-owned polling. UNKNOWN/NOT_FOUND remain observations until a terminal agent state or deadline.
     * New server completed receipts are retained at least 31 days; pending/running/indeterminate receipts
     * remain recoverable. Older receipts can be unavailable. INDETERMINATE needs trusted manual reconciliation;
     * this helper never resubmits a write or reruns an agent.
     */
    fun writeOutcomes(roomId: RoomId, writeRequestId: ByteArray, pollIntervalMillis: Long = 1_000, timeoutMillis: Long = 300_000): Flow<WriteOutcomeObservation> {
        require(writeRequestId.size in 16..64 && pollIntervalMillis > 0 && timeoutMillis > 0)
        val room = roomId.canonical(); val requestId = writeRequestId.copyOf()
        return flow {
            val deadline = kotlin.time.TimeSource.Monotonic.markNow()
            val caps = withTimeoutOrNull(timeoutMillis) {
                cachedCapabilities ?: sync.network(false) {
                    try { roomService.capabilities(CapabilitiesRequest()) }
                    catch (failure: RpcResponseException) {
                        if (failure.code == com.latenighthack.ktbuf.proto.Codes.UNIMPLEMENTED || failure.code == com.latenighthack.ktbuf.proto.Codes.NOT_FOUND) CapabilitiesResponse() else throw failure
                    }
                }.also { cachedCapabilities = it }
            }
            if (caps == null) { emit(WriteOutcomeObservation.Unavailable(WriteOutcomeObservation.UnavailableReason.DEADLINE_EXCEEDED)); return@flow }
            if (!caps.writeOutcomes) { emit(WriteOutcomeObservation.Unavailable(WriteOutcomeObservation.UnavailableReason.NOT_SUPPORTED)); return@flow }
            var last: GetWriteOutcomeResponse? = null
            while (currentCoroutineContext().isActive) {
                val remaining = timeoutMillis - deadline.elapsedNow().inWholeMilliseconds
                val response = if (remaining > 0) withTimeoutOrNull(remaining) {
                    sync.network(false) { roomService.getWriteOutcome(GetWriteOutcomeRequest(room, requestId.copyOf())) }
                } else null
                if (response == null) { emit(WriteOutcomeObservation.Unavailable(WriteOutcomeObservation.UnavailableReason.DEADLINE_EXCEEDED, last)); return@flow }
                last = response
                val observation = WriteOutcomeObservation.Response(response)
                emit(observation)
                if (observation.terminal) return@flow
                val next = timeoutMillis - deadline.elapsedNow().inWholeMilliseconds
                if (next > 0) delay(minOf(pollIntervalMillis, next))
            }
        }.distinctUntilChanged()
    }
    suspend fun awaitWriteOutcome(roomId: RoomId, writeRequestId: ByteArray, timeoutMillis: Long = 300_000): WriteOutcomeObservation =
        writeOutcomes(roomId, writeRequestId, timeoutMillis = timeoutMillis).first { it.terminal }

    /** Consume every cursor before recording an application watermark, including uninterested variants. */
    fun acceptedEventsAfter(cursor: Long): Flow<AcceptedConnectorEvent> = lockerStore.changesAfter(cursor).map { entry ->
        when (entry.kind) {
            1 -> AcceptedConnectorEvent.LockerChanged(entry.cursor, entry.toChange())
            2 -> AcceptedConnectorEvent.SessionEvent(entry.cursor, Event.fromByteArray(entry.payload))
            else -> error("Unsupported accepted event kind ${entry.kind}")
        }
    }
    fun notificationsAfter(cursor: Long): Flow<AcceptedNotification> = stream.eventsAfter(cursor).mapNotNull { accepted ->
        decodeNotification(accepted.event)?.let { AcceptedNotification(accepted.cursor, it) }
    }

    /** All change, notification, broadcast and raw-event consumers share the journal. Supply their minimum durable cursor. */
    suspend fun pruneAcceptedEventsThrough(cursor: Long) = lockerStore.pruneEventsThrough(cursor)

    fun changesAfter(cursor: Long): Flow<AcceptedLockerChange> = lockerStore.changesAfter(cursor).filter { it.kind == 1 }.map { AcceptedLockerChange(it.cursor, it.toChange()) }

    val changes: Flow<LockerUpdate>
        get() {
            return lockerStore.liveChanges().filter { it.kind == 1 }.map { it.toChange() }
        }

    /** Durable broadcasts; decoding happens in the collector, after transport acceptance and ACK. */
    fun broadcastsAfter(cursor: Long): Flow<IncomingBroadcast> = stream.eventsAfter(cursor).mapNotNull { accepted ->
        val event = accepted.event
        if (event.locker?.lockerId != null) return@mapNotNull null
        val room = event.roomId?.canonical() ?: return@mapNotNull null
        val id = event.eventId ?: return@mapNotNull null
        val payload = event.notification?.payload?.rawValue ?: return@mapNotNull null
        val context = BroadcastContext(room, id, event.notification?.push?.title, event.notification?.push?.body)
        val decoded = broadcastCodecs.decode(context, payload) ?: return@mapNotNull null
        IncomingBroadcast(accepted.cursor, context, decoded)
    }
    val broadcasts: Flow<IncomingBroadcast> get() = broadcastsAfter(0)

    val notifications: Flow<IncomingNotification>
        get() {
            return notificationsAfter(0).map { it.notification }
        }

    private val roomService = ShardedRoomServiceRpc(rpcClient)

    // When routing through a [RoutingRpcClient], a NOT_OWNER + redirect is recorded here so the
    // next write attempt (inside the same repeatWithBackoff loop) re-targets the owning node. With
    // a plain client (e.g. a monolith) this is a no-op and writes never see NOT_OWNER.
    private val routing = rpcClient as? RoutingRpcClient

    private fun recordRoomRedirect(roomId: RoomId, redirect: ShardRedirect?) {
        val owner = redirect?.ownerAddress ?: return
        routing?.recordRedirect(roomId.rawValue.toBase64String(), owner, redirect.epoch)
    }

    suspend fun start() {
        check(processingJob.isActive) { "LockerClient is closed" }
        if (!started.compareAndSet(false, true)) return
        stream.acceptEvent = { processEvent(it) }
        stream.acceptanceBoundary = { action -> withAcceptance { action() } }

        stream.hydrateSubscription = { room, session -> hydrateRoom(room, session) }
        processingScope.launch {
            repeatWithBackoff(exceptionHandler = { it !is CancellationException }) {
                lockerStore.pendingRatchets().forEach { recoverRatchet(it) }
                lockerStore.archivedRatchets().forEach { restoreArchivedRatchet(it) }; Unit
            }
        }
    }

    private suspend fun hydrateRoom(room: RoomId, session: SessionId? = stream.sessionId.value): List<IdentifiedLocker> {
        val room = room.canonical(); val session = session?.canonical()
        val capabilities = capabilities()
        // A replacement session must register itself even while an older hydration is in flight.
        return sync.read(room to session) {
        val before = lockerStore.getAllLockers(room)
        var watermark: Long? = null
        val lockers = mutableListOf<IdentifiedLocker>()
        var token = byteArrayOf()
        val seenTokens = mutableSetOf<String>()
        if (!capabilities.subscribeAndSnapshot && session != null) {
            val unsigned = SubscriptionRequest(roomId = room, sessionId = session, kind = SubscriptionRequest.OneOfKind.subscribe(SubscriptionRequest.Subscribe()))
            val response = roomService.subscription(unsigned.copy(proof = stream.signSessionRequest(SessionSigning.SUBSCRIPTION, session, unsigned.toByteArray())))
            check(response.result.isOk()) { "Subscription rejected" }
        }
        do {
            val page: List<IdentifiedLocker>
            val sequence: Long
            val next: ByteArray
            if (capabilities.subscribeAndSnapshot && session != null) {
                val unsigned = SubscribeAndSnapshotRequest(roomId = room, sessionId = session,
                    pageSize = if (capabilities.snapshotPaging) 64 else 0, pageToken = token)
                val response = roomService.subscribeAndSnapshot(unsigned.copy(proof = stream.signSessionRequest(SessionSigning.SNAPSHOT, session, unsigned.toByteArray())))
                check(response.result.isOk()) { "Subscribe and snapshot rejected" }
                page = response.lockers; sequence = response.roomSequence; next = response.nextPageToken
            } else {
                val response = roomService.getAllLockers(GetAllLockersRequest(roomId = room,
                    pageSize = if (capabilities.snapshotPaging) 64 else 0, pageToken = token))
                check(response.result.isOk()) { "Snapshot rejected" }
                page = response.lockers; sequence = response.roomSequence; next = response.nextPageToken
            }
            if (watermark == null) watermark = sequence else check(watermark == sequence) { "Snapshot watermark changed between pages" }
            lockers += page
            check(capabilities.snapshotPaging || next.isEmpty()) { "Unexpected unnegotiated snapshot page" }
            check(next.isEmpty() || seenTokens.add(next.toBase64String())) { "Snapshot page token repeated" }
            token = next
        } while (token.isNotEmpty())
        val present = lockers.mapNotNull { it.lockerId?.canonical() }.toSet()
        // Legacy snapshots omit tombstones. Confirm each omission; never infer deletion from a partial page.
        val missing = before.filter { LockerId(it.lockerIdRawValue, LockerKeyspace(it.lockerKeyspace)).canonical() !in present }
        val repairs = missing.map { stored ->
            val id = LockerId(stored.lockerIdRawValue, LockerKeyspace(stored.lockerKeyspace))
            val response = roomService.getLocker(GetLockerRequest(room, id))
            check(response.result.isOk()) { "Missing snapshot record confirmation rejected" }
            stored to response.locker
        }
        withAcceptance {
            lockerStore.acceptAtomically {
                lockers.forEach { acceptLocked(it.toUpdate(room, watermark ?: 0L)) }
                repairs.forEach { (expected, confirmed) ->
                    if (confirmed == null || (confirmed.locker == null && confirmed.version == 0L)) forgetUnchanged(expected)
                    else acceptLocked(confirmed.toUpdate(room))
                }
            }
        }
        lockers
        }
    }

    private var cachedCapabilities: CapabilitiesResponse? = null
    private suspend fun capabilities(): CapabilitiesResponse {
        // A warm write must not queue behind hydration merely to read cached capabilities.
        cachedCapabilities?.let { return it }
        return sync.read("capabilities") {
        cachedCapabilities ?: try { roomService.capabilities(CapabilitiesRequest()) }
        catch (e: RpcResponseException) {
            if (e.code == com.latenighthack.ktbuf.proto.Codes.UNIMPLEMENTED || e.code == com.latenighthack.ktbuf.proto.Codes.NOT_FOUND) CapabilitiesResponse() else throw e
        }.also { cachedCapabilities = it }
        }
    }

    class Change(val lockerId: LockerId, val transform: suspend (ByteArray) -> ByteArray)

    /** Atomic on capable servers. Legacy fallback is selected before submitting any write. */
    suspend fun updateLockers(roomId: RoomId, changes: List<Change>, initialKey: Secp256r1KeyPair? = null) = telemetry.observe(TelemetryOperation.CONNECTOR_BATCH_WRITE) { updateLockersObserved(roomId.canonical(), changes.map { Change(it.lockerId.canonical(), it.transform) }, initialKey) }

    private suspend fun updateLockersObserved(roomId: RoomId, changes: List<Change>, initialKey: Secp256r1KeyPair?) {
        require(changes.isNotEmpty() && changes.size <= 64 && changes.map { it.lockerId }.distinct().size == changes.size)
        // Room batch lock and stable locker ordering prevent overlapping single writes interleaving.
        suspend fun locked(index: Int, action: suspend () -> Unit) {
            if (index == changes.size) action()
            else sync.mutate(roomId to changes.sortedBy { it.lockerId.toByteArray().toBase64String() }[index].lockerId) { locked(index + 1, action) }
        }
        locked(0) {
            val caps = capabilities()
            if (!caps.postLockerChanges) {
                if (initialKey != null) {
                    val lock = lockLocker(roomId, LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM), initialKey, initialKey)
                    if (!lock.result.isOk()) throw LockerWriteException("initial room lock rejected: ${lock.result}")
                }
                coroutineScope { changes.map { change -> async {
                    updateLockerSerialized(roomId, change.lockerId, transform = change.transform)
                } }.awaitAll() }
                return@locked
            }
            val current = changes.associate { change ->
                val stored = lockerStore.getLocker(roomId, change.lockerId.keyspaceOrDefault(), change.lockerId)
                change.lockerId to ((stored?.version ?: 0L) to (stored?.lockerPayload ?: byteArrayOf()))
            }.toMutableMap()
            val scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM)
            val initialState = if (initialKey != null && caps.authorityV2) getLockScope(roomId, scope) else null
            val targetVersion = initialState?.scopeState?.lockVersion ?: 0L
            val authorityVersion = initialState?.parentState?.lockVersion ?: 0L
            val grant = initialKey?.let { key ->
                val public = key.publicKey.encode()
                val context = if (caps.authorityV2) LockerSigning.grantContextV2(roomId, scope, public, authorityVersion, targetVersion)
                    else LockerSigning.grantContext(roomId, scope, public)
                LockGrant(scope, Secp256R1Key.PublicKey(public), signatureOf(key, context, if (caps.authorityV2) 2 else 1), authorityVersion, targetVersion)
            }
            var submitted: PostLockerChangesRequest? = null
            try {
                repeatWithBackoff(retryLimit = WRITE_RETRY_LIMIT, exceptionHandler = WRITE_EXCEPTION_HANDLER) {
                    val request = submitted ?: run {
                        val authorities = if (caps.authorityV2 && initialKey == null) {
                            if (caps.getLockers) {
                                val result = sync.network { roomService.getLockers(GetLockersRequest(roomId, changes.map { it.lockerId })) }
                                require(result.results.size == changes.size) { "Incomplete batch authority discovery" }
                                changes.zip(result.results).associate { (change, response) ->
                                    check(response.result.isOk()) { "Authority discovery rejected" }
                                    change.lockerId to (response.locker?.lockState?.lockVersion ?: 0L)
                                }
                            } else changes.associate { it.lockerId to currentAuthorityVersion(roomId, it.lockerId) }
                        } else emptyMap()
                        PostLockerChangesRequest(roomId = roomId, initialLock = grant, parentLockVersion = targetVersion,
                            writeRequestId = kotlin.random.Random.nextBytes(32), changes = changes.map { change ->
                                val (version, plaintext) = current.getValue(change.lockerId)
                                val authority = if (initialKey != null) targetVersion + 1 else authorities[change.lockerId] ?: 0L
                                restoreArchivedKeyFor(roomId, change.lockerId)
                                val body = buildWriteBody(initialKey ?: lockKeySource?.writeKeyFor(roomId, change.lockerId),
                                    roomId, change.lockerId, version, change.transform(plaintext.copyOf()), authority, null, caps.authorityV2)
                                PostLockerChangeRequest(roomId = roomId, lockerId = change.lockerId, locker = body.locker,
                                    parentVersion = version, writeSignature = body.signature)
                            }).let { draft ->
                                val frozen = PostLockerChangesRequest.fromByteArray(draft.toByteArray())
                                if (frozen.toByteArray().size > minOf(8 * 1024 * 1024, caps.maxBatchBytes))
                                    throw LockerWriteException("atomic locker batch exceeds encoded request limit")
                                submitted = frozen
                                frozen
                            }
                    }
                    val response = sync.network { roomService.postLockerChanges(request) }
                    when (response.result) {
                        is PostLockerChangesResponse.Result.OK -> {
                            check(response.changes.size == request.changes.size && response.changes.all { it.result.isOk() }) { "Incomplete committed batch metadata" }
                            withAcceptance { lockerStore.acceptAtomically {
                                request.changes.zip(response.changes).forEach { (change, result) ->
                                    acceptCommitted(LockerUpdate(roomId, change.lockerId!!, result.version, change.locker!!.plaintextPayload()), request.writeRequestId,
                                        request.changes.zip(response.changes).map { (source, committed) -> WriteSourceVersion(source.lockerId, committed.version) })
                                }
                            } }
                            if (response.agentFailed || response.agentPending || response.agentIndeterminate) throw LockerSourceCommittedException(
                                response.changes.maxOfOrNull { it.version } ?: 0, response.agentPending,
                                message = "Batch source committed; agent ${if (response.agentIndeterminate) "indeterminate" else if (response.agentPending) "pending" else "failed"}",
                                roomId = roomId, writeRequestId = request.writeRequestId,
                                sourceVersions = request.changes.zip(response.changes).map { (change, result) -> WriteSourceVersion(change.lockerId, result.version) },
                                agentIndeterminate = response.agentIndeterminate)
                        }
                        is PostLockerChangesResponse.Result.CONFLICT -> {
                        telemetry.safeRecord(TelemetryOperation.CONNECTOR_CONFLICT, TelemetryOutcome.CONFLICT)
                            response.changes.forEachIndexed { index, item ->
                                if (item.result is PostLockerChangeResponse.Result.UPDATE_LOCAL_VERSION)
                                    current[changes[index].lockerId] = item.version to (item.existingLocker?.plaintextPayload() ?: byteArrayOf())
                                else if (!item.result.isOk()) throw LockerWriteException("batch rejected: ${item.result}")
                            }
                            if (response.changes.isEmpty()) throw LockerWriteException("initial lock conflict")
                            submitted = null
                            retry()
                        }
                        is PostLockerChangesResponse.Result.NOT_OWNER -> { recordRoomRedirect(roomId, response.redirect); retry() }
                        else -> throw LockerWriteException("batch rejected: ${response.result}")
                    }
                }
            } catch (e: RetryLimitExceeded) { throw LockerWriteException("batch retry budget exhausted: $e") }
        }
    }

    private suspend fun acceptCommitted(update: LockerUpdate, requestId: ByteArray = byteArrayOf(), sources: List<WriteSourceVersion> = listOf(WriteSourceVersion(update.lockerId, update.version))) {
        try { accept(update) }
        catch (failure: ConnectorRetentionExceededException) {
            throw LockerSourceCommittedException(sources.maxOfOrNull { it.version } ?: update.version, false, "Source committed at version ${update.version}; local acceptance needs retention capacity", failure,
                roomId = update.roomId, writeRequestId = requestId, sourceVersions = sources)
        }
    }

    private suspend fun accept(update: LockerUpdate): Boolean = withAcceptance { acceptLocked(update) }

    private suspend fun acceptLocked(update: LockerUpdate): Boolean {
        val stored = lockerStore.getLocker(update.roomId, update.lockerId.keyspaceOrDefault(), update.lockerId)
        if (stored != null && (stored.version > update.version ||
            (stored.roomSequence > 0 && update.roomSequence > 0 && stored.roomSequence >= update.roomSequence) ||
            (stored.version == update.version && (stored.deleted || stored.lockerPayload.contentEquals(update.payload))))) return false
        lockerStore.acceptLocker(update.toStored())
        watchMutex.withLock { watched[update.roomId to update.lockerId.keyspaceOrDefault()]?.revision?.update { it + 1 } }
        return true
    }

    /** Complete immutable cache snapshots. Live notifications are conflated wakeups, never state. */
    internal fun watchSnapshot(roomId: RoomId, keyspace: LockerKeyspace): Flow<List<IdentifiedLocker>> {
        val roomId = roomId.canonical(); val keyspace = keyspace.canonical()
        return flow {
            val identity = roomId to keyspace
            val entry = watchMutex.withLock { check(watched.size < 1_024 || watched.containsKey(identity)) { "Snapshot watcher admission limit exceeded" }; watched.getOrPut(identity) { SnapshotWatch() }.also { it.users++ } }
            suspend fun snapshot() = acceptance.withLock {
                lockerStore.getAllLockers(roomId, keyspace).filterNot { it.deleted }.map { it.toIdentifiedLocker() }
            }
            try {
                coroutineScope {
                    val hydration = launch {
                        try { subscribeToRoom(roomId, false); fetchAllLockers(roomId, keyspace) }
                        catch (e: CancellationException) { throw e }
                        catch (e: Exception) { log.error { "watch hydration failed: $e" } }
                    }
                    try {
                        // Cached history is emitted as a whole. If empty, finish initial hydration first.
                        if (snapshot().isEmpty()) hydration.join()
                        emitAll(entry.revision.map { snapshot() }.distinctUntilChanged())
                    } finally { hydration.cancel() }
                }
            } finally {
                withContext(NonCancellable) { watchMutex.withLock {
                    if (--entry.users == 0 && watched[identity] === entry) watched.remove(identity)
                } }
            }
        }
    }

    private suspend fun processEvent(event: Event): Boolean {
        val roomId = event.roomId?.canonical() ?: return true
        val identified = event.locker
        val lockerId = identified?.lockerId?.canonical()
        val version = identified?.version ?: 0L
        val body = identified?.locker
        val hasBody = body != null
        if (body != null && !LockerEnvelope.isSupported(body)) return false

        if (lockerId != null) {
            if (hasBody) {
                accept(LockerUpdate(roomId, lockerId, version, body!!.plaintextPayload(), deleted = false, roomSequence = event.roomSequence))
            } else {
                // A body-less locker event is a tombstone: the server signals a
                // delete by sending lockerId + version with no Locker body.
                accept(LockerUpdate(roomId, lockerId, version, byteArrayOf(), deleted = true, roomSequence = event.roomSequence))
            }
        }


        return true
    }

    private suspend fun decodeNotification(event: Event): IncomingNotification? {
        val roomId = event.roomId?.canonical() ?: return null
        val lockerId = event.locker?.lockerId?.canonical()
        val notificationPayload = event.notification?.payload?.rawValue
        if (lockerId != null && notificationPayload != null) {
            val keyspace = lockerId.keyspaceOrDefault()
            val decoded = if (codecs.isEmpty(keyspace)) {
                notificationPayload
            } else {
                val push = event.notification?.push
                codecs.decode(
                    NotificationContext(roomId, lockerId, keyspace, push?.title, push?.body),
                    notificationPayload,
                )
            }
            // A codec may drop the notification by returning null.
            if (decoded != null) {
                return IncomingNotification(roomId, lockerId, decoded)
            }
        }
        return null
    }

    /**
     * Builds the write-side [Notification] from the caller's [build] block, then
     * runs its payload through the encode chain so a consumer's codec round-trips
     * with [processEvent]'s decode.
     */
    private suspend fun encodedNotification(
        roomId: RoomId,
        lockerId: LockerId,
        configure: NotificationBuilder.() -> Unit,
    ): Notification {
        // Pass `configure` to the factory directly; `Notification { configure() }`
        // would instead resolve to NotificationBuilder.build()'s sibling and drop it.
        val built = Notification(configure)
        val payload = built.payload?.rawValue
        val keyspace = lockerId.keyspaceOrDefault()
        if (payload == null || payload.isEmpty() || codecs.isEmpty(keyspace)) {
            return built
        }
        val encoded = codecs.encode(
            NotificationContext(roomId, lockerId, keyspace, built.push?.title, built.push?.body),
            payload,
        )
        return built.copy { payload { rawValue = encoded } }
    }

    fun stop() {
        processingJob.cancel()
    }

    suspend fun closeAndJoin() { stop(); processingJob.join() }

    suspend fun subscribeToRoom(roomId: RoomId, waitForSubscription: Boolean = true) {
        stream.subscribe(roomId.canonical(), waitForSubscription)
    }

    suspend fun unsubscribeFromRoom(roomId: RoomId) {
        stream.unsubscribe(roomId.canonical())
    }

    suspend fun getAllLockers(roomId: RoomId, keyspace: LockerKeyspace, revalidate: Boolean = true): List<IdentifiedLocker> {
        val roomId = roomId.canonical(); val keyspace = keyspace.canonical()
        val cached = lockerStore.getAllLockers(roomId, keyspace)

        if (cached.isNotEmpty()) {
            if (revalidate) {
                processingScope.launch {
                    // background revalidation is best-effort; a network failure must not
                    // surface as an uncaught crash
                    try {
                        fetchAllLockers(roomId, keyspace)
                    } catch (e: kotlinx.coroutines.CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        log.error { "revalidate failed for ${roomId.toLogString()}: $e" }
                    }
                }
            }
            return cached.filterNot { it.deleted }.map { it.toIdentifiedLocker() }
        }

        return fetchAllLockers(roomId, keyspace)
    }

    /**
     * Every locker currently in the local cache, across all rooms and keyspaces — the
     * client's whole known-locker set, for introspection/debugging. Reads the cache only
     * (no server round-trip); each entry carries its room, keyspaced id, version, and
     * plaintext bytes.
     */
    suspend fun getAllKnownLockers(): List<LockerUpdate> =
        lockerStore.getAllLockers().filterNot { it.deleted }.map { stored ->
            LockerUpdate(
                RoomId(stored.roomIdRawValue.copyOf()),
                LockerId(stored.lockerIdRawValue.copyOf(), LockerKeyspace { value = stored.lockerKeyspace }),
                stored.version,
                stored.lockerPayload.copyOf(),
            )
        }

    private suspend fun fetchAllLockers(roomId: RoomId, keyspace: LockerKeyspace): List<IdentifiedLocker> = telemetry.observe(TelemetryOperation.CONNECTOR_GET_ALL) {
        hydrateRoom(roomId).filter { it.locker != null && it.lockerId?.keyspaceOrDefault() == keyspace } }

    suspend fun getLocker(roomId: RoomId, lockerId: LockerId, revalidate: Boolean = true): IdentifiedLocker? {
        val roomId = roomId.canonical(); val lockerId = lockerId.canonical()
        val cached = lockerStore.getLocker(roomId, lockerId.keyspaceOrDefault(), lockerId)

        if (cached != null) {
            if (revalidate) {
                processingScope.launch {
                    // background revalidation is best-effort; a network failure must not
                    // surface as an uncaught crash
                    try {
                        fetchLocker(roomId, lockerId)
                    } catch (e: kotlinx.coroutines.CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        log.error { "revalidate failed for ${roomId.toLogString()}: $e" }
                    }
                }
            }
            return cached.takeUnless { it.deleted }?.toIdentifiedLocker()
        }

        return fetchLocker(roomId, lockerId)
    }

    suspend fun getLockers(roomId: RoomId, lockerIds: List<LockerId>): List<IdentifiedLocker?> {
        val roomId = roomId.canonical(); val lockerIds = lockerIds.map { it.canonical() }
        require(lockerIds.size <= 64)
        val caps = capabilities()
        if (!caps.getLockers) return coroutineScope { lockerIds.map { id -> async { fetchLocker(roomId, id) } }.awaitAll() }
        return sync.read(Triple("lockers", roomId, lockerIds.map { it.canonical() })) {
            val before = lockerIds.map { lockerStore.getLocker(roomId, it.keyspaceOrDefault(), it) }
            val response = roomService.getLockers(GetLockersRequest(roomId, lockerIds.map { it.canonical() }))
            check(response.results.size == lockerIds.size) { "Incomplete bulk read response" }
            response.results.mapIndexed { index, item ->
                check(item.result.isOk()) { "Locker read rejected" }
                acceptRead(roomId, item.locker, before[index])
            }
        }
    }

    private suspend fun forgetUnchanged(expected: StoredLocker) {
        val room = RoomId(expected.roomIdRawValue); val id = LockerId(expected.lockerIdRawValue, LockerKeyspace(expected.lockerKeyspace))
        if (lockerStore.getLocker(room, id.keyspaceOrDefault(), id) != expected) return
        lockerStore.forgetLocker(expected)
        watchMutex.withLock { watched[room to id.keyspaceOrDefault()]?.revision?.update { it + 1 } }
    }

    private suspend fun acceptRead(room: RoomId, identified: IdentifiedLocker?, before: StoredLocker?): IdentifiedLocker? {
        withAcceptance {
            if (identified == null || (identified.locker == null && identified.version == 0L)) {
                before?.let { forgetUnchanged(it) }
            } else acceptLocked(identified.toUpdate(room))
        }
        return identified?.takeIf { it.locker != null }?.let { it.copy(lockerId = it.lockerId?.canonical()) }
    }

    private suspend fun fetchLocker(roomId: RoomId, lockerId: LockerId): IdentifiedLocker? = telemetry.observe(TelemetryOperation.CONNECTOR_GET) { sync.read(roomId.canonical() to lockerId.canonical()) {
        val before = lockerStore.getLocker(roomId, lockerId.keyspaceOrDefault(), lockerId)
        val response = roomService.getLocker(GetLockerRequest(roomId, lockerId.canonical()))
        check(response.result.isOk()) { "Locker read rejected" }
        acceptRead(roomId, response.locker, before)
    } }

    /**
     * Delete the cached version, fetching once when this client has no cached version.
     * A conflict refreshes the cache and throws [LockerDeleteConflictException]; the caller
     * must decide whether to delete the newly observed content. Ambiguous retries reuse the
     * original request, receipt, signature and notification, including on legacy servers.
     */
    suspend fun deleteLocker(roomId: RoomId, lockerId: LockerId, notificationBuilder: NotificationBuilder.() -> Unit = {}) =
        roomId.canonical().let { room -> lockerId.canonical().let { id -> sync.mutate(room to id) { telemetry.observe(TelemetryOperation.CONNECTOR_DELETE) { deleteLockerSerialized(room, id, notificationBuilder) } } } }

    private suspend fun deleteLockerSerialized(
        roomId: RoomId,
        lockerId: LockerId,
        notificationBuilder: NotificationBuilder.() -> Unit = {}
    ) {
        var cached = lockerStore.getLocker(roomId, lockerId.keyspaceOrDefault(), lockerId)
        if (cached == null) {
            fetchLocker(roomId, lockerId)
            cached = lockerStore.getLocker(roomId, lockerId.keyspaceOrDefault(), lockerId)
        }
        val parentVersion = cached?.version ?: 0L
        restoreArchivedKeyFor(roomId, lockerId)
        val signingKey = lockKeySource?.writeKeyFor(roomId, lockerId)
        val caps = capabilities()
        val notif = encodedNotification(roomId, lockerId) { this.notificationBuilder() }
        val authority = if (signingKey != null && caps.authorityV2) currentAuthorityVersion(roomId, lockerId) else 0L
        val signature = signingKey?.let { signWrite(it, roomId, lockerId, parentVersion, ByteArray(0), authority, notif, caps.authorityV2) }
        val request = DeleteLockerRequest.fromByteArray(DeleteLockerRequest {
            this.roomId = roomId; this.lockerId = lockerId; this.parentVersion = parentVersion
            this.writeSignature = signature; this.notification = notif
            if (caps.deleteReceipts) writeRequestId = kotlin.random.Random.nextBytes(32)
        }.toByteArray())
        var legacyAmbiguity = false
        val deletedVersion = try {
            repeatWithBackoff(retryLimit = WRITE_RETRY_LIMIT, exceptionHandler = { failure ->
                val retryable = WRITE_EXCEPTION_HANDLER(failure)
                if (retryable && !caps.deleteReceipts) legacyAmbiguity = true
                retryable
            }) {
                val result = sync.network { roomService.deleteLocker(request) }
                when (result.result) {
                    is DeleteLockerResponse.Result.OK -> {
                        if (caps.deleteReceipts && !result.writeRequestId.contentEquals(request.writeRequestId)) {
                            acceptCommitted(LockerUpdate(roomId, lockerId, result.version, byteArrayOf(), deleted = true), request.writeRequestId)
                            throw LockerSourceCommittedException(result.version, false, "Delete committed but its receipt identifier is invalid",
                                roomId = roomId, writeRequestId = request.writeRequestId,
                                sourceVersions = listOf(WriteSourceVersion(lockerId, result.version)))
                        }
                        result.version
                    }
                    is DeleteLockerResponse.Result.UPDATE_LOCAL_VERSION -> {
                        telemetry.safeRecord(TelemetryOperation.CONNECTOR_CONFLICT, TelemetryOutcome.CONFLICT)
                        var repairFailure: Throwable? = null
                        try { fetchLocker(roomId, lockerId) }
                        catch (cancelled: CancellationException) { throw cancelled }
                        catch (failure: Exception) { repairFailure = failure }
                        throw LockerDeleteConflictException(parentVersion, result.version, legacyAmbiguity, repairFailure)
                    }
                    is DeleteLockerResponse.Result.NOT_OWNER -> { recordRoomRedirect(roomId, result.redirect); retry() }
                    is DeleteLockerResponse.Result.REQUEST_ID_REUSED -> throw LockerWriteException("delete receipt identifier was reused with different request bytes")
                    is DeleteLockerResponse.Result.SIGNATURE_REQUIRED -> throw LockerWriteException("locker is locked; a signing key is required to delete")
                    is DeleteLockerResponse.Result.SIGNATURE_INVALID -> throw LockerWriteException("locker delete signature was rejected")
                    is DeleteLockerResponse.Result.NOT_AUTHORIZED -> throw LockerWriteException("locker delete not authorized")
                    else -> { if (!caps.deleteReceipts) legacyAmbiguity = true; retry() }
                }
            }
        } catch (e: RetryLimitExceeded) {
            throw LockerWriteException("locker delete failed after $WRITE_RETRY_LIMIT attempts: $e")
        }
        deletedVersion?.let {
            acceptCommitted(LockerUpdate(roomId, lockerId, it, byteArrayOf(), deleted = true), request.writeRequestId)
        }
    }

    /**
     * Update a locker's plaintext content. [transform] receives the current plaintext
     * and returns the new plaintext; when the locker is signed (a key is available from
     * the [LockKeySource]) the result is wrapped in a signed envelope and the write is
     * signed. The signature binds the parent version, so a version conflict re-runs
     * [transform] and re-signs against the fresh version — preserving the fair-read
     * retry. Set [ratchet] to rotate the signing key atomically with this write.
     *
     * Throws [LockerWriteException] for retry exhaustion, terminal rejection, or a committed
     * source whose synchronous agent did not complete successfully.
     */
    suspend fun updateLocker(
        roomId: RoomId, lockerId: LockerId,
        notificationBuilder: NotificationBuilder.(Locker?) -> Unit = {}, ratchet: Boolean = false,
        transform: suspend (ByteArray) -> ByteArray,
    ): Locker? = roomId.canonical().let { room -> lockerId.canonical().let { id -> sync.mutate(room to id) { telemetry.observe(TelemetryOperation.CONNECTOR_WRITE) { updateLockerSerialized(room, id, notificationBuilder, ratchet, transform) } } } }

    private suspend fun updateLockerSerialized(
        roomId: RoomId,
        lockerId: LockerId,
        notificationBuilder: NotificationBuilder.(Locker?) -> Unit = {},
        ratchet: Boolean = false,
        transform: suspend (ByteArray) -> ByteArray,
    ): Locker? {
        lockerStore.pendingRatchets().filter { it.request.roomId == roomId && it.request.lockerId?.canonical() == lockerId.canonical() }.forEach { recoverRatchet(it, serialized = true) }
        val cached = lockerStore.getLocker(roomId, lockerId.keyspaceOrDefault(), lockerId)
        var currentPlaintext = cached?.toIdentifiedLocker()?.locker?.plaintextPayload() ?: byteArrayOf()
        var parentVersion = cached?.version ?: 0L

        restoreArchivedKeyFor(roomId, lockerId)
        val signingKey = lockKeySource?.writeKeyFor(roomId, lockerId)
        val pendingRatchetKey = if (ratchet && signingKey != null) Secp256r1KeyPair.generate() else null

        // Names the write in rejection messages: a REQUIRED verdict means the key chain returned no
        // signing key for this room (signed=false), which is otherwise invisible from the message.
        val writeContext = "room=${roomId.toLogString()} locker=${lockerId.toLogString()} signed=${signingKey != null}"

        val caps = capabilities()
        val supportsReceipts = caps.writeReceipts
        if (pendingRatchetKey != null && !supportsReceipts) throw LockerWriteException("Ratchets require durable server write receipts")
        var submitted: PostLockerChangeRequest? = null
        var committedAgentStatus: PostLockerChangeResponse? = null
        var committedResponse: PostLockerChangeResponse? = null
        val updatedLocker = try {
            repeatWithBackoff(retryLimit = WRITE_RETRY_LIMIT, exceptionHandler = WRITE_EXCEPTION_HANDLER) {
                val request = submitted ?: run {
                    val authority = if (signingKey != null && caps.authorityV2) currentAuthorityVersion(roomId, lockerId) else 0L
                    val newPlaintext = transform(currentPlaintext.copyOf())
                    // The builder sees a provisional payload envelope; the final V2 signature binds its encoded notification.
                    val provisional = buildWriteBody(signingKey, roomId, lockerId, parentVersion, newPlaintext)
                    val notif = encodedNotification(roomId, lockerId) { this.notificationBuilder(provisional.locker) }
                    val body = if (caps.authorityV2) buildWriteBody(signingKey, roomId, lockerId, parentVersion, newPlaintext, authority, notif, true) else provisional
                    val ratchetMsg = if (pendingRatchetKey != null && signingKey != null)
                        buildRatchet(signingKey, pendingRatchetKey, roomId, lockerId, parentVersion, authority, caps.authorityV2) else null
                    PostLockerChangeRequest {
                        if (supportsReceipts) writeRequestId = kotlin.random.Random.nextBytes(32)
                        this.roomId = roomId; this.lockerId = lockerId; this.parentVersion = parentVersion
                        this.locker = body.locker; this.writeSignature = body.signature
                        this.ratchet = ratchetMsg; this.notification = notif
                    }.let { draft ->
                        val frozen = PostLockerChangeRequest.fromByteArray(draft.toByteArray())
                        if (pendingRatchetKey != null) lockerStore.saveRatchet(PendingRatchet(frozen, pendingRatchetKey.privateKey.encode()))
                        submitted = frozen
                        frozen
                    }
                }
                val result = sync.network { roomService.postLockerChange(request) }

                if (result.result.isOk()) committedResponse = result
                if (result.result.isOk() && (result.agentFailed || result.agentPending || result.agentIndeterminate)) committedAgentStatus = result
                log.debug { "updated locker=${lockerId.toLogString()} result=${result.result} version=${result.version}" }

                val locker = when (result.result) {
                    is PostLockerChangeResponse.Result.OK -> request.locker!!
                    is PostLockerChangeResponse.Result.UPDATE_LOCAL_VERSION -> {
                        telemetry.safeRecord(TelemetryOperation.CONNECTOR_CONFLICT, TelemetryOutcome.CONFLICT)
                        submitted?.let { if (pendingRatchetKey != null) lockerStore.clearRatchet(it) }
                        submitted = null
                        currentPlaintext = result.existingLocker?.plaintextPayload() ?: byteArrayOf()
                        parentVersion = result.version
                        retry()
                    }
                    is PostLockerChangeResponse.Result.NOT_OWNER -> {
                        // This node doesn't own the room's shard; cache the redirect and retry so
                        // the routing client re-targets the owner on the next attempt.
                        recordRoomRedirect(roomId, result.redirect)
                        retry()
                    }
                    is PostLockerChangeResponse.Result.SIGNATURE_REQUIRED ->
                        throw LockerWriteException("locker is locked; a signing key is required ($writeContext)")
                    is PostLockerChangeResponse.Result.SIGNATURE_INVALID ->
                        throw LockerWriteException("locker write signature was rejected ($writeContext)")
                    is PostLockerChangeResponse.Result.NOT_AUTHORIZED ->
                        throw LockerWriteException("locker write not authorized ($writeContext)")
                    else -> {
                        // Unknown/transient server result: the write may have persisted before the
                        // server failed (e.g. a fan-out error), so a blind retry with the same
                        // parentVersion would just bounce off UPDATE_LOCAL_VERSION and burn two
                        // attempts per round trip. Rebase on the server's current state first.
                        if (supportsReceipts) throw LockerWriteException("write rejected: ${result.result}")
                        submitted = null
                        fetchLocker(roomId, lockerId)?.let { fetched ->
                            currentPlaintext = fetched.locker?.plaintextPayload() ?: byteArrayOf()
                            parentVersion = fetched.version
                        }
                        retry()
                    }
                }

                IdentifiedLocker {
                    this.locker = locker
                    this.lockerId = lockerId
                    this.version = result.version
                }
            }
        } catch (e: RetryLimitExceeded) {
            throw LockerWriteException("locker update failed after $WRITE_RETRY_LIMIT attempts ($writeContext): $e")
        }

        val result = updatedLocker ?: return null
        var adoptionPending: RatchetAdoptionPendingException? = null
        if (pendingRatchetKey != null) {
            val request = requireNotNull(submitted)
            try { adoptCommittedRatchet(PendingRatchet(request, pendingRatchetKey.privateKey.encode()), requireNotNull(committedResponse)) }
            catch (pending: RatchetAdoptionPendingException) { adoptionPending = pending }
        }
        val update = result.toUpdate(roomId)
        acceptCommitted(update, requireNotNull(submitted).writeRequestId)
        adoptionPending?.let { throw it }
        committedAgentStatus?.let { throw LockerSourceCommittedException(it.version, it.agentPending,
            message = "Source committed at version ${it.version}; agent ${if (it.agentIndeterminate) "indeterminate" else if (it.agentPending) "pending" else "failed"}",
            roomId = roomId, writeRequestId = requireNotNull(submitted).writeRequestId,
            sourceVersions = listOf(WriteSourceVersion(lockerId, it.version)), agentIndeterminate = it.agentIndeterminate) }

        return result.locker
    }

    private suspend fun recoverRatchet(pending: PendingRatchet, serialized: Boolean = false) {
        val request = pending.request
        val room = requireNotNull(request.roomId)
        val id = requireNotNull(request.lockerId)
        suspend fun resolve() {
            if (lockerStore.pendingRatchets().none { it.request.writeRequestId.contentEquals(request.writeRequestId) }) return
            val response = sync.network { roomService.postLockerChange(request) }
            when (response.result) {
                is PostLockerChangeResponse.Result.OK -> {
                    var adoptionPending: RatchetAdoptionPendingException? = null
                    try { adoptCommittedRatchet(pending, response) } catch (failure: RatchetAdoptionPendingException) { adoptionPending = failure }
                    accept(LockerUpdate(room, id, response.version, requireNotNull(request.locker).plaintextPayload()))
                    adoptionPending?.let { throw it }
                }
                is PostLockerChangeResponse.Result.NOT_OWNER -> {
                    recordRoomRedirect(room, response.redirect)
                    throw IllegalStateException("Ratchet recovery needs current owner")
                }
                is PostLockerChangeResponse.Result.UPDATE_LOCAL_VERSION,
                is PostLockerChangeResponse.Result.SIGNATURE_INVALID,
                is PostLockerChangeResponse.Result.SIGNATURE_REQUIRED,
                is PostLockerChangeResponse.Result.NOT_AUTHORIZED -> lockerStore.clearRatchet(request)
                else -> throw IllegalStateException("Unresolved ratchet receipt: ${response.result}")
            }
        }
        if (serialized) resolve() else sync.mutate(room to id.canonical()) { resolve() }
    }

    private fun adoptionPending(archive: ArchivedRatchet, target: LockerId, cause: Throwable? = null) =
        RatchetAdoptionPendingException(archive.version, cause, archive.room, archive.pending.request.writeRequestId, listOf(WriteSourceVersion(target, archive.version)))

    private suspend fun adoptCommittedRatchet(pending: PendingRatchet, response: PostLockerChangeResponse) {
        val archive = ArchivedRatchet(pending, response.lockState, response.version)
        try { lockerStore.archiveRatchet(archive) }
        catch (cancelled: CancellationException) { throw cancelled }
        catch (failure: Exception) { throw adoptionPending(archive, requireNotNull(archive.pending.request.lockerId), failure) }
        ratchetAdoption.withLock { adoptKey(archive, requireNotNull(pending.request.lockerId)) }
        lockerStore.clearRatchet(pending.request)
    }

    private suspend fun adoptKey(archive: ArchivedRatchet, target: LockerId) {
        val source = lockKeySource ?: throw adoptionPending(archive, target)
        val newKey = requireNotNull(Secp256r1KeyPair.fromPrivateKey(archive.pending.privateKey)) { "Invalid archived ratchet key" }
        val current = source.writeKeyFor(archive.room, target)?.publicKey?.encode()
        if (current?.contentEquals(archive.publicKey) == true) return
        val old = archive.pending.request.writeSignature?.publicKey?.rawValue
        // Never replace a provider's unrelated/newer key with an old completed transition.
        if (current != null && old != null && !current.contentEquals(old)) throw adoptionPending(archive, target)
        try { source.onRatcheted(archive.room, target, newKey) }
        catch (cancelled: CancellationException) { throw cancelled }
        catch (failure: Exception) { throw adoptionPending(archive, requireNotNull(archive.pending.request.lockerId), failure) }
        val resolved = source.writeKeyFor(archive.room, target)?.publicKey?.encode()
        if (resolved?.contentEquals(archive.publicKey) != true) throw adoptionPending(archive, target)
    }

    private suspend fun restoreArchivedRatchet(archive: ArchivedRatchet, target: LockerId = requireNotNull(archive.pending.request.lockerId)) {
        ratchetAdoption.withLock {
            val caps = capabilities()
            val current = if (caps.authorityV2) getLockScope(archive.room, archive.scope).scopeState else
                sync.network { roomService.getLocker(GetLockerRequest(archive.room, target.canonical())) }.locker?.lockState
            if (current?.publicKey?.rawValue?.contentEquals(archive.publicKey) != true) {
                // Exact V2 scope history proves this key obsolete; an overridden legacy locker does not.
                if (caps.authorityV2 && current != null && current.lockVersion >= (archive.state?.lockVersion ?: 0)) lockerStore.forgetArchivedRatchet(archive)
                return
            }
            adoptKey(archive, target)
        }
    }

    private suspend fun restoreArchivedKeyFor(room: RoomId, id: LockerId) {
        if (!lockerStore.hasArchivedRatchet(room)) return
        val authority = sync.network { roomService.getLocker(GetLockerRequest(room, id.canonical())) }.locker?.lockState?.publicKey?.rawValue ?: return
        lockerStore.matchingRatchet(room, authority)?.let { restoreArchivedRatchet(it, id) }
    }

    /** Establish a lock at [scope] with [keyPair]. Sign the grant with [parentKeyPair]
     *  (the parent-scope or room key); pass null for a TOFU root in a non-public-keyed room. */
    suspend fun lockLocker(
        roomId: RoomId,
        scope: LockScope,
        keyPair: Secp256r1KeyPair,
        parentKeyPair: Secp256r1KeyPair? = null,
        parentLockVersion: Long = 0L
    ): LockLockerResponse {
        val roomId = roomId.canonical(); val scope = scope.canonical()
        val caps = capabilities()
        val state = if (caps.authorityV2) getLockScope(roomId, scope) else null
        val targetVersion = if (caps.authorityV2 && parentLockVersion == 0L) state?.scopeState?.lockVersion ?: 0L else parentLockVersion
        val authorityVersion = state?.parentState?.lockVersion ?: 0L
        val publicKeyBytes = keyPair.publicKey.encode()
        val parentSignature = parentKeyPair?.let {
            val context = if (caps.authorityV2) LockerSigning.grantContextV2(roomId, scope, publicKeyBytes, authorityVersion, targetVersion)
                else LockerSigning.grantContext(roomId, scope, publicKeyBytes)
            signatureOf(it, context, if (caps.authorityV2) 2 else 1)
        }

        return roomService.lockLocker(LockLockerRequest {
            this.roomId = roomId
            this.parentLockVersion = targetVersion
            grant = LockGrant(
                scope = scope,
                publicKey = Secp256R1Key.PublicKey(rawValue = publicKeyBytes),
                parentSignature = parentSignature,
                authorityVersion = authorityVersion, scopeVersion = targetVersion,
            )
        }).also {
            if (it.result is LockLockerResponse.Result.NOT_OWNER) recordRoomRedirect(roomId, it.redirect)
        }
    }

    /** Remove the lock at [scope], authorized by its current [keyPair]. */
    suspend fun unlockLocker(
        roomId: RoomId,
        scope: LockScope,
        keyPair: Secp256r1KeyPair,
        parentLockVersion: Long
    ): UnlockLockerResponse {
        val roomId = roomId.canonical(); val scope = scope.canonical()
        val v2 = capabilities().authorityV2
        val context = if (v2) LockerSigning.unlockContextV2(roomId, scope, parentLockVersion) else LockerSigning.unlockContext(roomId, scope)
        return roomService.unlockLocker(UnlockLockerRequest {
            this.roomId = roomId
            this.scope = scope
            this.parentLockVersion = parentLockVersion
            signature = signatureOf(keyPair, context, if (v2) 2 else 1)
        }).also {
            if (it.result is UnlockLockerResponse.Result.NOT_OWNER) recordRoomRedirect(roomId, it.redirect)
        }
    }

    suspend fun getLockScope(roomId: RoomId, scope: LockScope): GetLockScopeResponse =
        sync.network { roomService.getLockScope(GetLockScopeRequest(roomId.canonical(), scope.canonical())) }.also { check(it.result.isOk()) { "Authority discovery rejected" } }

    private suspend fun currentAuthorityVersion(roomId: RoomId, lockerId: LockerId): Long {
        val response = sync.network { roomService.getLocker(GetLockerRequest(roomId, lockerId.canonical())) }
        check(response.result.isOk()) { "Authority discovery failed: ${response.result}" }
        return response.locker?.lockState?.lockVersion ?: 0L
    }

    private class WriteBody(val locker: Locker, val signature: Signature?)

    private suspend fun buildWriteBody(
        signingKey: Secp256r1KeyPair?,
        roomId: RoomId,
        lockerId: LockerId,
        parentVersion: Long,
        plaintext: ByteArray,
        lockVersion: Long = 0, notification: Notification? = null, v2: Boolean = false,
    ): WriteBody {
        if (signingKey == null) {
            return WriteBody(Locker { open { encodedPayload = plaintext } }, null)
        }
        val hash = SHA256.digest(plaintext)
        val signature = signWrite(signingKey, roomId, lockerId, parentVersion, hash, lockVersion, notification, v2)
        val locker = Locker {
            sealed {
                payload {
                    this.checksum = hash
                    enclosure {
                        this.signature = signature
                        innerPayload = plaintext
                    }
                }
            }
        }
        return WriteBody(locker, signature)
    }

    private suspend fun buildRatchet(
        oldKey: Secp256r1KeyPair,
        newKey: Secp256r1KeyPair,
        roomId: RoomId,
        lockerId: LockerId,
        parentVersion: Long,
        lockVersion: Long = 0, v2: Boolean = false,
    ): PostLockerChangeRequest.Ratchet {
        val newPublicKeyBytes = newKey.publicKey.encode()
        val context = if (v2) LockerSigning.ratchetContextV2(roomId, lockerId, parentVersion, lockVersion, newPublicKeyBytes, emptyList())
            else LockerSigning.ratchetContext(roomId, lockerId, parentVersion, newPublicKeyBytes)
        return PostLockerChangeRequest.Ratchet(
            newPublicKey = Secp256R1Key.PublicKey(rawValue = newPublicKeyBytes),
            newSharedKeys = emptyList(),
            signature = signatureOf(oldKey, context, if (v2) 2 else 1),
        )
    }

    private suspend fun signWrite(
        keyPair: Secp256r1KeyPair,
        roomId: RoomId,
        lockerId: LockerId,
        parentVersion: Long,
        contentHash: ByteArray,
        lockVersion: Long = 0, notification: Notification? = null, v2: Boolean = false,
    ): Signature = signatureOf(keyPair, if (v2) LockerSigning.writeContextV2(roomId, lockerId, parentVersion, lockVersion, contentHash, notification)
        else LockerSigning.writeContext(roomId, lockerId, parentVersion, contentHash), if (v2) 2 else 1)

    private suspend fun signatureOf(keyPair: Secp256r1KeyPair, message: ByteArray, version: Int = 1): Signature =
        Signature(
            publicKey = Secp256R1Key.PublicKey(rawValue = keyPair.publicKey.encode()),
            // ktcrypto's sign() returns the raw r‖s the server expects on every platform.
            signature = keyPair.privateKey.sign(message),
            signingVersion = version,
        )
}

private fun RoomId.toLogString() = "r+" + (this?.rawValue?.toBase64String()?.take(6) ?: "(nul)")

private fun LockerId?.toLogString() = "l+" + (this?.rawValue?.toBase64String()?.take(6) ?: "(nul)") +
    "/ks" + (this?.keyspace?.value?.toString() ?: "0")

/** Stable local cursor for a committed locker change. */
data class AcceptedLockerChange(val cursor: Long, val change: LockerClient.LockerUpdate)
