package com.latenighthack.lockers.connector

import com.latenighthack.ktbuf.rpc.BackoffContext
import com.latenighthack.ktbuf.net.RpcResponseException
import kotlinx.coroutines.CancellationException

/** Invalid wire data/configuration and finite packet limits require a new caller decision.
 * RPC temporary capacity and transport failures retain the transport's retry policy. */
internal fun isRetryableProtocolFailure(failure: Throwable): Boolean {
    if (failure is CancellationException) throw failure
    if (failure is IllegalArgumentException || failure is IndexOutOfBoundsException) return false
    return BackoffContext.DEFAULT_EXCEPTION_HANDLER(failure)
}

/** A remote error body must not multiply into an unbounded retained status map. */
internal fun retainedProtocolFailure(failure: Throwable): Throwable =
    if (failure is RpcResponseException && (failure.path.length > 2_048 || failure.verb.length > 64 || failure.errorMessage.length > 2_048))
        RpcResponseException(failure.path.take(2_048), failure.verb.take(64), failure.code, failure.errorMessage.take(2_048))
    else failure
