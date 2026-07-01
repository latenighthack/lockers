package com.latenighthack.lockers.server.tools

import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.takeWhile

private data class CollectNext<T>(val isDone: Boolean, val value: T)

fun <T> Flow<T>.takeWhileInclusive(predicate: suspend (T) -> Boolean): Flow<T> = flow {
    collect {
        if (!predicate(it)) {
            emit(CollectNext(false, it))
            emit(CollectNext(true, it))
        } else {
            emit(CollectNext(false, it))
        }
    }
}
    .takeWhile {
        !it.isDone
    }
    .map {
        it.value
    }

suspend fun <T, U: Any> Flow<T>.collectFirst(firstCallback: suspend (T) -> U?, callback: suspend (U, T) -> Unit) {
    var isFirst = true
    lateinit var chainValue: U

    collect { value ->
        if (isFirst) {
            isFirst = false
            chainValue = firstCallback(value) ?: awaitCancellation()
        } else {
            callback(chainValue, value)
        }
    }
}
