package com.latenighthack.lockers.server

import com.latenighthack.lockers.common.v1.Push
import com.latenighthack.lockers.push.v1.PushRegistration
import com.latenighthack.lockers.server.services.push.v1.providers.PushResult
import com.latenighthack.lockers.server.services.push.v1.providers.WebPushProvider
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertNull
import kotlin.test.assertNotNull
import java.net.InetAddress
import com.latenighthack.lockers.server.services.push.v1.providers.WebPushEndpointPolicy
import kotlin.test.assertIs

class WebPushEndpointTest {
    @Test fun `DNS checks all addresses and trusted domains keep exact boundaries`() {
        val public = InetAddress.getByAddress(byteArrayOf(8,8,8,8))
        val private = InetAddress.getByAddress(byteArrayOf(127,0,0,1))
        assertNull(WebPushEndpointPolicy(setOf("fcm.googleapis.com")) { listOf(public) }.rejection("https://fcm.googleapis.com/send/token"))
        assertNotNull(WebPushEndpointPolicy(setOf("fcm.googleapis.com")) { listOf(public, private) }.rejection("https://fcm.googleapis.com/send/token"))
        assertNotNull(WebPushEndpointPolicy(setOf("*.notify.windows.com")) { listOf(public) }.rejection("https://evilenotify.windows.com/send/token"))
        assertNull(WebPushEndpointPolicy(setOf("*.notify.windows.com")) { listOf(public) }.rejection("https://x.notify.windows.com/send/token"))
    }

    @Test fun `untrusted endpoints are rejected before sender initialization`() = runBlocking {
        val provider = WebPushProvider(WebPushConfig(null, null, null))
        for (endpoint in listOf("http://127.0.0.1/metadata", "https://127.0.0.1/", "https://[::1]/", "https://169.254.169.254/", "https://attacker.invalid/", "https://fcm.googleapis.com.attacker.invalid/", "https://fcm.googleapis.com:8443/", "https://user:password@fcm.googleapis.com/")) {
            val registration = PushRegistration { backend.webPush { this.endpoint = endpoint } }
            assertIs<PushResult.Rejected>(provider.send(registration, Push { title = "secret" }), endpoint)
        }
    }
    @Test fun `DNS outages remain retryable without invalidating the credential`(): Unit = runBlocking {
        val provider = WebPushProvider(WebPushConfig(null, null, null)) { throw java.net.UnknownHostException("temporary DNS outage") }
        assertIs<PushResult.Retryable>(provider.send(PushRegistration { backend.webPush { endpoint = "https://fcm.googleapis.com/send/token" } }, Push { title = "secret" }))
    }

}
