package com.latenighthack.lockers.server.tools

import com.latenighthack.ktstore.IndexName
import com.latenighthack.ktstore.OrderedKeyEncoding
import com.latenighthack.ktstore.StorageCodec
import com.latenighthack.ktstore.StoreKey

object EpochMillisCodec : StorageCodec<Long, ByteArray> {
    override fun encode(value: Long) = OrderedKeyEncoding.long(value)

    override fun key(name: IndexName) = StoreKey.SerializedKey(name.value)
}

data class QueueSnapshot(
    val depth: Long,
    val unknownAge: Long,
    val oldestAt: Long?,
    val recipients: Long = depth,
    val eligibleDepth: Long? = null,
    val eligibleAt: Long? = null,
)
