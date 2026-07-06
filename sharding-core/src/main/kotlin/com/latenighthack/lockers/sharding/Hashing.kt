package com.latenighthack.lockers.sharding

// FNV-1a 64-bit + a SplitMix64 finalizer: deterministic, allocation-light, and dependency-free —
// identical output on every node and JVM, which is what makes routing decisions agree cluster-wide.
// Plain FNV-1a alone avalanches poorly when inputs share a long common prefix and differ only in the
// trailing byte (exactly our case: keyspaceTag ++ shard/room bytes). The finalizer scatters those
// near-identical hashes across the whole ring so distribution and rebalance stay even.
private val FNV_OFFSET_BASIS = 0xCBF29CE484222325uL.toLong()
private const val FNV_PRIME = 0x100000001B3L

internal fun fnv1a64(bytes: ByteArray): Long {
    var h = FNV_OFFSET_BASIS
    for (b in bytes) {
        h = h xor (b.toLong() and 0xFF)
        h *= FNV_PRIME // wraps mod 2^64, as FNV requires
    }
    return mix64(h)
}

/** SplitMix64 finalizer: a bijective, strong-avalanche mix so near-identical inputs scatter. */
internal fun mix64(value: Long): Long {
    var z = value
    z = (z xor (z ushr 30)) * 0xBF58476D1CE4E5B9uL.toLong()
    z = (z xor (z ushr 27)) * 0x94D049BB133111EBuL.toLong()
    return z xor (z ushr 31)
}

/** Big-endian 8-byte encoding of a keyspace, folded into hashes so equal keys in different
 *  keyspaces land on independent shards. */
internal fun keyspaceTag(keyspace: Long): ByteArray = ByteArray(8) { i ->
    (keyspace ushr (8 * (7 - i)) and 0xFF).toByte()
}

/** Big-endian 4-byte encoding of an int, used for ring-point derivation. */
internal fun intBytes(value: Int): ByteArray = ByteArray(4) { i ->
    (value ushr (8 * (3 - i)) and 0xFF).toByte()
}
