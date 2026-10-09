package com.latenighthack.lockers.server

import io.netty.buffer.ByteBuf
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http2.Http2FrameCodecBuilder
import kotlin.test.*

class NetworkDependencyContractTest {
    @Test fun resolvedJdbcAndNettyArtifactsMatchReviewedSecurityReleases() {
        assertEquals("42.7.14", Class.forName("org.postgresql.util.DriverInfo").getField("DRIVER_VERSION").get(null))
        // Shaded gRPC bundles retain stale, unrelocated version-resource names.
        // Verify the actual loaded unshaded codec/transport classes instead.
        listOf(ByteBuf::class.java, EmbeddedChannel::class.java, Http2FrameCodecBuilder::class.java)
            .forEach { assertEquals("4.1.139.Final", it.`package`.implementationVersion, it.name) }
    }

    @Test fun alignedHttp2ClientWritesAValidConnectionPrefaceAndReleasesBuffers() {
        val channel = EmbeddedChannel(Http2FrameCodecBuilder.forClient().build())
        try {
            val preface = channel.readOutbound<ByteBuf>()
            try { assertEquals("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n", preface.toString(Charsets.US_ASCII)) }
            finally { preface.release() }
        } finally { channel.finishAndReleaseAll() }
    }
}
