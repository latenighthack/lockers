package com.latenighthack.lockers.common

/** Wire fanout can be small while its paired durable inbox rows are large. */
object InboxAdmission {
    const val MAX_BATCH_EXPANSION_BYTES = 64L * 1024 * 1024
    /** Conservative legacy row + full-event sidecar + fixed receipt/header footprint. */
    fun recipientExpansionBytes(eventBytes: Int, sessionBytes: Int): Long {
        require(eventBytes >= 0 && sessionBytes >= 0)
        return 2L * eventBytes + 2L * sessionBytes + 1024
    }
}
