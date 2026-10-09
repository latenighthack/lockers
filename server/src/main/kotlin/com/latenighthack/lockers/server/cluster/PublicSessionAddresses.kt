package com.latenighthack.lockers.server.cluster

import com.latenighthack.lockers.sharding.NodeId
import java.net.URI

/** Operator-declared public WebSocket endpoints, separate from credentialed east-west addresses. */
object PublicSessionAddresses {
    fun parse(raw: String?, nodes: Set<NodeId>): Map<NodeId, String> {
        require(!raw.isNullOrBlank()) { "LOCKERS_PUBLIC_SESSION_ADDRS must map every ring node to its public session endpoint" }
        val addresses = linkedMapOf<NodeId, String>()
        for (entry in raw.split(',')) {
            val pair = entry.trim().split('=', limit = 2)
            require(pair.size == 2 && pair[0].isNotBlank()) { "Invalid LOCKERS_PUBLIC_SESSION_ADDRS entry" }
            val node = NodeId(pair[0].trim())
            val address = validate(pair[1].trim())
            require(addresses.put(node, address) == null) { "Duplicate node in LOCKERS_PUBLIC_SESSION_ADDRS" }
        }
        require(addresses.keys.containsAll(nodes)) { "LOCKERS_PUBLIC_SESSION_ADDRS must cover the complete ring node set" }
        return addresses.toMap()
    }

    fun validate(address: String): String {
        require(address.isNotEmpty() && address.length <= 2048 && address.none { it.isWhitespace() || it.isISOControl() }) { "Invalid public session endpoint" }
        val uri = try { URI(if (address.contains("://")) address else "http://$address") }
            catch (_: Exception) { throw IllegalArgumentException("Invalid public session endpoint") }
        require(uri.scheme in setOf("http", "https") && !uri.host.isNullOrBlank() && uri.userInfo == null &&
            uri.rawQuery == null && uri.rawFragment == null && (uri.port == -1 || uri.port in 1..65535)) { "Invalid public session endpoint" }
        return address
    }
}
