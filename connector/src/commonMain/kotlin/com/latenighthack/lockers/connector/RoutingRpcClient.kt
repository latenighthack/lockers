package com.latenighthack.lockers.connector

import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.ktbuf.net.RpcMethodSpecifier
import com.latenighthack.ktbuf.net.RpcResponse
import com.latenighthack.ktbuf.net.RpcServerStream
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.update

/**
 * A smart-client [RpcClient] that follows server-provided ownership redirects. The service stubs
 * already stamp each routable call with its shard key metadata — `rid=base64(roomId)` on room RPCs
 * ([com.latenighthack.lockers.connector.internal.ShardedRoomServiceRpc]) and `s=base64(sessionId)`
 * on the session watch ([com.latenighthack.lockers.connector.internal.ShardedSessionServiceRpc]).
 * This client reads that metadata and dispatches the call to the node currently cached as the owner
 * of that shard key, falling back to the [seed] node when the owner is unknown.
 *
 * Routing is driven ENTIRELY by the server: the client holds no ring logic. When a room write is
 * answered `NOT_OWNER` + `ShardRedirect`, or a session open is answered `EPOCH_STALE` +
 * `ShardRedirect`, the decode layer that saw the (decoded) response calls [recordRedirect] with the
 * shard key and the `owner_address` the server handed back. The next call for that key then routes
 * to the owner. Because the room write path ([LockerClient]) already retries a non-terminal result
 * inside its `repeatWithBackoff` loop, and the [Stream] reconnect loop re-opens on a retryable
 * result, recording the redirect is enough for the very next attempt/reconnect to land on the owner
 * with no ring knowledge and no bespoke retry here.
 *
 * @param seed the node to reach before any owner is known (the client's configured entry point).
 * @param clientFactory builds (and this client caches) one delegate [RpcClient] per owner address;
 *   the platform supplies e.g. `{ url -> HttpRpcClient(url) }`. Kept as a parameter so this stays
 *   multiplatform and unit-testable with a fake transport.
 * @param normalizeAddress maps a server `owner_address` (`host:port` or a URL) to the key/URL the
 *   [clientFactory] expects; defaults to prefixing `http://` when the address is bare host:port.
 */
class RoutingRpcClient(
    private val seed: RpcClient,
    private val clientFactory: (address: String) -> RpcClient,
    private val normalizeAddress: (String) -> String = ::defaultNormalizeAddress,
) : RpcClient {
    // Lock-free, thread-safe state via atomic StateFlow updates (the pattern used across the
    // connector); no platform lock primitive is needed in common code.
    private val ownerByKey = MutableStateFlow<Map<String, Owner>>(emptyMap())
    private val clientsByAddress = MutableStateFlow<Map<String, RpcClient>>(emptyMap())

    /** A cached shard owner: the normalized [address] and the [epoch] the server resolved it at. */
    private data class Owner(val address: String, val epoch: Long)

    /**
     * Record that [ownerAddress] owns [routingKey] (the base64 `rid`/`s` the stubs emit), as of
     * shard-map [epoch]. Called by the decode layer on a `NOT_OWNER` / `EPOCH_STALE` redirect. An
     * empty/blank address clears any cached owner so the key falls back to the [seed]. A redirect
     * at a lower epoch than the one already cached is ignored (staleness ordering). Idempotent and
     * safe to call concurrently.
     */
    fun recordRedirect(routingKey: String, ownerAddress: String, epoch: Long = 0L) {
        if (ownerAddress.isBlank()) {
            ownerByKey.update { it - routingKey }
            return
        }
        val owner = Owner(normalizeAddress(ownerAddress), epoch)
        ownerByKey.update { current ->
            val existing = current[routingKey]
            if (existing != null && existing.epoch > owner.epoch) current
            else current + (routingKey to owner)
        }
    }

    /**
     * Where a call routes right now: the delegate [client] plus the [method] to send — the original
     * method against the [seed] when no owner is known, or one with the resolved epoch stamped on
     * (alongside the stub's `rid`/`s`, so the target can detect a stale route) against the owner.
     * Resolves the owner exactly once per call.
     */
    private fun route(method: RpcMethodSpecifier): Pair<RpcClient, RpcMethodSpecifier> {
        val owner = routingKeyOf(method)?.let { ownerByKey.value[it] } ?: return seed to method
        val stamped = method.copy(
            additionalParameters = method.additionalParameters + ("e" to owner.epoch.toString()),
        )
        return clientFor(owner.address) to stamped
    }

    /** The delegate for [address], built once and cached (a concurrent build is harmless). */
    private fun clientFor(address: String): RpcClient {
        clientsByAddress.value[address]?.let { return it }
        val created = clientFactory(address)
        clientsByAddress.update { if (it.containsKey(address)) it else it + (address to created) }
        return clientsByAddress.value[address] ?: created
    }

    override suspend fun unaryCall(
        method: RpcMethodSpecifier,
        headers: Map<String, String>,
        request: ByteArray,
    ): RpcResponse {
        val (client, routedMethod) = route(method)
        return client.unaryCall(routedMethod, headers, request)
    }

    override suspend fun serverStreamingCall(
        method: RpcMethodSpecifier,
        block: suspend RpcServerStream.() -> Unit,
        readyCallback: () -> Unit,
    ) {
        val (client, routedMethod) = route(method)
        return client.serverStreamingCall(routedMethod, block, readyCallback)
    }

    private companion object {
        /** The shard-key the stubs attach: `rid` for room calls, `s` for the session watch. */
        fun routingKeyOf(method: RpcMethodSpecifier): String? =
            method.additionalParameters["rid"] ?: method.additionalParameters["s"]
    }
}

/** Prefix a bare `host:port` with `http://`; leave anything that already looks like a URL alone. */
internal fun defaultNormalizeAddress(address: String): String =
    if (address.contains("://")) address else "http://$address"
