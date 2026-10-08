package com.latenighthack.lockers.server.services.push.v1.providers

import java.net.InetAddress
import java.net.URI

/**
 * Browser-supplied URLs cannot choose arbitrary HTTP destinations. Only operator-trusted push
 * services are allowed. The vendor HTTP client's default redirect policy is NEVER; a redirect
 * therefore never escapes this policy. Host trust also excludes attacker-controlled rebinding
 * names; DNS addresses are checked again immediately before every send.
 */
internal class WebPushEndpointPolicy(
    private val allowedHosts: Set<String>,
    private val resolve: (String) -> List<InetAddress> = { InetAddress.getAllByName(it).toList() },
) {
    fun rejection(endpoint: String): String? {
        val uri = try { URI(endpoint) } catch (_: Exception) { return "invalid web push endpoint" }
        val host = uri.host?.lowercase() ?: return "web push endpoint requires a host"
        if (uri.scheme != "https" || uri.port !in listOf(-1, 443) || uri.userInfo != null || uri.fragment != null)
            return "web push endpoint requires HTTPS on port 443 without credentials or fragment"
        if (allowedHosts.none { trusted -> host == trusted || (trusted.startsWith("*.") && host.endsWith(trusted.substring(1)) && host != trusted.substring(2)) })
            return "web push endpoint host is not a trusted push service"
        val addresses = try { resolve(host) } catch (_: Exception) { return "web push endpoint DNS unavailable" }
        if (addresses.isEmpty() || addresses.any { !isPublic(it) }) return "web push endpoint resolves to a non-public address"
        return null
    }

    private fun isPublic(address: InetAddress): Boolean {
        if (address.isAnyLocalAddress || address.isLoopbackAddress || address.isLinkLocalAddress || address.isSiteLocalAddress || address.isMulticastAddress) return false
        val b = address.address.map { it.toInt() and 255 }
        if (b.size == 4) {
            val a = b[0]; val c = b[1]
            return !(a == 0 || a == 10 || a == 127 || a >= 224 || (a == 100 && c in 64..127) ||
                (a == 169 && c == 254) || (a == 172 && c in 16..31) || (a == 192 && (c == 168 || c == 0 || (c == 2))) ||
                (a == 198 && (c in 18..19 || (c == 51 && b[2] == 100))) || (a == 203 && c == 0 && b[2] == 113))
        }
        return b.size == 16 && b[0] and 0xfe != 0xfc && !(b[0] == 0x20 && b[1] == 0x01 && b[2] == 0x0d && b[3] == 0xb8) &&
            !(b.take(12).all { it == 0 })
    }
}
