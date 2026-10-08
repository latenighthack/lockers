package com.latenighthack.lockers.server

import io.grpc.CallOptions
import io.grpc.MethodDescriptor
import io.grpc.ServerServiceDefinition
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.stub.ClientCalls
import io.grpc.stub.ServerCalls
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertContentEquals

/** Exercise the relocated HTTP/2 transport; direct Netty tests cannot verify this artifact. */
class RelocatedGrpcContractTest {
    @Test fun patchedRelocatedTransportRoundTripsUnaryBytesAndCloses() {
        val marshaller = object : MethodDescriptor.Marshaller<ByteArray> {
            override fun stream(value: ByteArray): InputStream = ByteArrayInputStream(value)
            override fun parse(stream: InputStream): ByteArray = stream.readBytes()
        }
        val method = MethodDescriptor.newBuilder<ByteArray, ByteArray>()
            .setType(MethodDescriptor.MethodType.UNARY).setFullMethodName("review.Echo/Bytes")
            .setRequestMarshaller(marshaller).setResponseMarshaller(marshaller).build()
        val service = ServerServiceDefinition.builder("review.Echo")
            .addMethod(method, ServerCalls.asyncUnaryCall<ByteArray, ByteArray> { request, observer ->
                observer.onNext(request); observer.onCompleted()
            }).build()
        val server = NettyServerBuilder.forPort(0).addService(service).build().start()
        val channel = NettyChannelBuilder.forAddress("127.0.0.1", server.port).usePlaintext().build()
        try {
            val bytes = byteArrayOf(0, -1, 127)
            assertContentEquals(bytes, ClientCalls.blockingUnaryCall(channel, method,
                CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS), bytes))
        } finally {
            channel.shutdownNow(); channel.awaitTermination(5, TimeUnit.SECONDS)
            server.shutdownNow(); server.awaitTermination(5, TimeUnit.SECONDS)
        }
    }
}
