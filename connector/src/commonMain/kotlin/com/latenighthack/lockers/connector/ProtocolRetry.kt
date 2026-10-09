package com.latenighthack.lockers.connector

import com.latenighthack.ktbuf.rpc.BackoffContext
import kotlinx.coroutines.CancellationException

/** Invalid wire data/configuration and finite packet limits require a new caller decision.
 * RPC temporary capacity and transport failures retain the transport's retry policy. */
internal fun isRetryableProtocolFailure(failure: Throwable): Boolean {
    if (failure is CancellationException) throw failure
    if (failure is IllegalArgumentException || failure is IndexOutOfBoundsException) return false
    return BackoffContext.DEFAULT_EXCEPTION_HANDLER(failure)
}
