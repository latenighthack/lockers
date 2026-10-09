package com.latenighthack.lockers.server.tools

import com.latenighthack.ktbuf.net.GrpcRequestContext
import com.latenighthack.ktbuf.net.ServerDescriptor
import com.latenighthack.ktbuf.net.ServerMethod
import com.latenighthack.ktbuf.net.ServerMethodDescriptor
import com.latenighthack.lockers.room.v1.GetLockerRequest
import com.latenighthack.lockers.room.v1.GetLockerResponse
import com.latenighthack.lockers.room.v1.readFrom
import com.latenighthack.lockers.room.v1.writeTo
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.config.MeterFilter
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class RpcHandlerMetricsTest {
    private suspend fun invoke(registry: SimpleMeterRegistry,
        action: suspend () -> GetLockerResponse): GetLockerResponse {
        val descriptor = ServerDescriptor("test", "Test", listOf(
            ServerMethodDescriptor<GetLockerRequest, GetLockerResponse, Any>("Get",
                GetLockerRequest.Companion::readFrom,
                GetLockerResponse::writeTo, false, false, ServerMethod.Unary { _, _ -> action() }),
        )).measured(registry)
        @Suppress("UNCHECKED_CAST")
        val method = descriptor.methods.single() as ServerMethodDescriptor<Any, Any, Any>
        val context = GrpcRequestContext("", emptyMap(), emptyMap(), emptyMap(), descriptor, method)
        return (method.handler as ServerMethod.Unary).handler.invoke(Any(), context,
            GetLockerRequest()) as GetLockerResponse
    }

    @Test fun applicationErrorsAndCancellationAreCountedAndActiveRequestsReturnToZero() = runBlocking {
        val registry = SimpleMeterRegistry()
        try {
            val response = invoke(registry) { GetLockerResponse(result = GetLockerResponse.Result.UNKNOWN_ERROR) }
            assertEquals(GetLockerResponse.Result.UNKNOWN_ERROR, response.result)
            assertEquals(1, registry.get("fullhouse.rpc.duration").tag("outcome", "error").timer().count())
            assertFailsWith<CancellationException> { invoke(registry) { throw CancellationException("stopped") } }
            assertEquals(1, registry.get("fullhouse.rpc.duration").tag("outcome", "cancelled").timer().count())
            assertEquals(0, registry.get("fullhouse.rpc.active").longTaskTimer().activeTasks())
        } finally { registry.close() }
    }

    @Test fun registryFailureCannotReplaceSuccessfulResponse() = runBlocking {
        val registry = SimpleMeterRegistry()
        try {
            registry.config().meterFilter(object : MeterFilter {
                override fun map(id: Meter.Id): Meter.Id = error("metrics registry unavailable")
            })
            assertEquals(GetLockerResponse.Result.OK, invoke(registry) { GetLockerResponse() }.result)
        } finally { registry.close() }
    }
}
