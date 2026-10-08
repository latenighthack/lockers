package com.latenighthack.lockers.connector

import com.latenighthack.lockers.common.v1.SessionId
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.flow.*

sealed interface StreamConnectionState {
    data object Connecting : StreamConnectionState
    data class Connected(val sessionId: SessionId, val epoch: Long) : StreamConnectionState
    data class Retrying(val reason: String?) : StreamConnectionState
    data class Failed(val error: StreamFatalError) : StreamConnectionState
    data class Closed(val error: StreamFatalError? = null) : StreamConnectionState
}
class StreamClosedException : IllegalStateException("Client stream is closed")
class StreamFailedException(val error: StreamFatalError) : IllegalStateException(error.reason)

/** Synchronous projections retain StateFlow.value consistency without an extra coroutine. */
internal class MappedStateFlow<S, T>(private val source: StateFlow<S>, private val project: (S) -> T) : StateFlow<T> {
    override val value: T get() = project(source.value)
    override val replayCache: List<T> get() = listOf(value)
    override suspend fun collect(collector: FlowCollector<T>): Nothing {
        source.map(project).distinctUntilChanged().collect(collector)
        awaitCancellation()
    }
}
