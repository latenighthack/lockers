package com.latenighthack.lockers.server.tools

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.flow
import kotlin.experimental.ExperimentalTypeInference

abstract class BaseServiceImpl {
    @OptIn(ExperimentalTypeInference::class)
    protected fun <T> serviceLifecycleFlow(@BuilderInference block: suspend FlowCollector<T>.() -> Unit): Flow<T> {
        return flow(block)
    }
}
