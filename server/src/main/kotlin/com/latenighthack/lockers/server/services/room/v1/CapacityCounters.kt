package com.latenighthack.lockers.server.services.room.v1

import com.latenighthack.ktstore.Database
import com.latenighthack.ktstore.IndexName
import com.latenighthack.ktstore.Store
import com.latenighthack.ktstore.StoreDefinition
import com.latenighthack.ktstore.StoreName
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream

class CapacityCounter(val key: ByteArray, val value: Long)

private fun encodeCounter(value: CapacityCounter): ByteArray = ByteArrayOutputStream().use { buffer ->
    DataOutputStream(buffer).use { out ->
        require(value.key.size in 1..MAX_COUNTER_KEY_BYTES && value.value >= 0)
        out.writeInt(value.key.size); out.write(value.key); out.writeLong(value.value)
    }; buffer.toByteArray()
}
private fun decodeCounter(bytes: ByteArray): CapacityCounter =
    DataInputStream(ByteArrayInputStream(bytes)).use { input ->
    val size = input.readInt(); require(size in 1..MAX_COUNTER_KEY_BYTES && size <= input.available())
    val key = ByteArray(size).also(input::readFully); val value = input.readLong()
    require(value >= 0 && input.available() == 0); CapacityCounter(key, value)
}
private const val MAX_COUNTER_KEY_BYTES = 256

/** Row counts that admission checks read instead of COUNT scans. */
object CapacityCounterDefinitionV1 : StoreDefinition<CapacityCounter>(StoreName("capacity_counters_v1"),
    "capacity-counter-v1", ::decodeCounter, ::encodeCounter) {
    val id = bytesIndex(IndexName("counter"), CapacityCounter::key, "raw-counter-v1").also { primaryKey(it) }
}

/**
 * Admission counters updated in the same transaction as the rows they count.
 *
 * Each family is written only while its [lockKey] is held, the same logical lock that already
 * serialised the COUNT-then-insert admission. A missing counter is initialised from COUNT, so
 * existing databases need no data migration. Callers read counters before changing counted rows
 * in a transaction, so initialisation never observes that transaction's own change.
 */
class CapacityCounters internal constructor(private val database: Database) :
    Store<CapacityCounter>(database, CapacityCounterDefinitionV1) {
    class Family(val name: String, val lockKey: String) {
        private val prefix = name.encodeToByteArray()
        init { require(prefix.isNotEmpty() && 0 !in prefix) }
        fun globalKey() = prefix + byteArrayOf(0, 0)
        fun roomKey(room: ByteArray) = prefix + byteArrayOf(0, 1) + room
        fun global(initial: suspend () -> Long) = Counter(globalKey(), initial)
        fun room(room: ByteArray, initial: suspend () -> Long) = Counter(roomKey(room), initial)
        internal val lower get() = prefix + byteArrayOf(0)
        internal val upper get() = prefix + byteArrayOf(0) + ByteArray(MAX_COUNTER_KEY_BYTES) { -1 }
    }
    class Counter internal constructor(val key: ByteArray, val initial: suspend () -> Long) {
        init { require(key.size <= MAX_COUNTER_KEY_BYTES) }
    }

    /** Counter rows read and written by one transaction, flushed once before it commits. */
    private class Batch(val database: Database, val prefetch: List<Pair<Family, ByteArray>>) :
        AbstractCoroutineContextElement(Key) {
        companion object Key : CoroutineContext.Key<Batch>
        val values = HashMap<List<Byte>, Long>()
        val loaded = HashSet<List<Byte>>()
        val dirty = LinkedHashMap<List<Byte>, CapacityCounter>()
    }
    private suspend fun batch() = coroutineContext[Batch]?.takeIf { it.database === database }

    /**
     * Runs [block] with one read and one write for every counter it touches. [prefetch] names
     * counters the transaction is likely to need; they are fetched with the first read, but only
     * for families whose lock the transaction already holds, so a cached value cannot go stale.
     * Must run inside the transaction whose locks guard the counters; nested calls join.
     */
    suspend fun <T> batched(prefetch: List<Pair<Family, ByteArray>>, block: suspend () -> T): T {
        if (batch() != null) return block()
        val batch = Batch(database, prefetch)
        return withContext(batch) {
            val result = block()
            if (batch.dirty.isNotEmpty()) saveAll(batch.dirty.values.toList())
            result
        }
    }

    /** One read for every counter; a missing counter is counted once from its rows. */
    suspend fun read(counters: List<Counter>): LongArray {
        val batch = batch()
        if (batch == null) {
            val found = getMany(counters.map { CapacityCounterDefinitionV1.id.eq(it.key) })
                .associate { it.key.toList() to it.value }
            return LongArray(counters.size) { found[counters[it].key.toList()] ?: counters[it].initial() }
        }
        val unknown = counters.map { it.key.toList() }.filter { it !in batch.loaded }
        if (unknown.isNotEmpty()) {
            val held = heldLockKeys()
            val likely = batch.prefetch.filter { (family, _) -> family.lockKey in held }.map { it.second.toList() }
            val keys = (unknown + likely).distinct().filter { it !in batch.loaded }
            getMany(keys.map { CapacityCounterDefinitionV1.id.eq(it.toByteArray()) })
                .forEach { batch.values[it.key.toList()] = it.value }
            batch.loaded.addAll(keys)
        }
        return LongArray(counters.size) { index ->
            val key = counters[index].key.toList()
            batch.values[key] ?: counters[index].initial().also { batch.values[key] = it }
        }
    }
    /** The stored value, or null while the counter is uninitialised (it then recounts on first [read]). */
    suspend fun stored(counter: Counter): Long? = get(CapacityCounterDefinitionV1.id.eq(counter.key))?.value
    /** Writes values previously returned by [read] plus their change. */
    suspend fun write(counters: List<Counter>, values: LongArray) {
        require(counters.size == values.size)
        val rows = counters.mapIndexed { index, counter ->
            CapacityCounter(counter.key, values[index].coerceAtLeast(0))
        }
        val batch = batch() ?: return saveAll(rows)
        rows.forEach { batch.values[it.key.toList()] = it.value; batch.dirty[it.key.toList()] = it }
    }
    suspend fun adjust(counters: List<Counter>, delta: Long) {
        val values = read(counters)
        write(counters, LongArray(values.size) { values[it] + delta })
    }

    /**
     * Forgets a family's counters so they are recounted from their rows. Deleting a counter is
     * always safe under the family lock; this repairs counts written by a different version.
     */
    suspend fun reset(family: Family) {
        while (database.transaction(family.lockKey) {
                val batch = CapacityCounterDefinitionV1.id.query(RESET_BATCH,
                    lower = family.lower, upper = family.upper)
                database.deleteBatch(CapacityCounterDefinitionV1.storeName, batch)
            } > 0) kotlinx.coroutines.currentCoroutineContext().ensureActive()
    }

    companion object {
        private const val RESET_BATCH = 256
        val agentWork = Family("agent_work", AGENT_WORK_LOCK)
        val lockers = Family("lockers", LOCKER_ADMISSION_LOCK)
        fun outbox(prefix: String) = Family("${prefix}_outbox_retained", outboxQuotaLock(prefix))
        /** Counters a room commit may touch: its outbox, agent journal and new lockers. */
        fun roomScope(room: ByteArray, outboxPrefix: String) = listOf(outbox(outboxPrefix), agentWork, lockers)
            .flatMap { family -> listOf(family to family.roomKey(room), family to family.globalKey()) }
        /** Every family maintained by this server, for a full recount. */
        fun families() = listOf(agentWork, lockers, outbox("delivery"), outbox("push_delivery"))
    }
}

internal const val AGENT_WORK_LOCK = "lockers.agent-work"
internal const val LOCKER_ADMISSION_LOCK = "lockers.locker-admission"
internal fun outboxQuotaLock(prefix: String) = "lockers.$prefix-outbox-quota"
