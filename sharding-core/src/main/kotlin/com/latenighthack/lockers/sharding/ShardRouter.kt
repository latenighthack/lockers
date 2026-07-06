package com.latenighthack.lockers.sharding

import com.latenighthack.lockers.sharding.spi.Membership
import com.latenighthack.lockers.sharding.spi.PeerLocator
import com.latenighthack.lockers.sharding.spi.ShardMapSource
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach

/** The resolved owner of a partition key, on a specific ring, at that ring's epoch. */
data class Route(
    val isLocal: Boolean,
    val node: NodeId,
    val address: PeerAddress?,
    val epoch: Epoch,
)

/**
 * One ring's live view: its [Membership] (and hence this node's identity on that ring) plus the
 * latest [ShardMap] tracked from its [ShardMapSource].
 */
internal class RingView(
    val membership: Membership,
    private val source: ShardMapSource,
    initial: ShardMap,
) {
    @Volatile
    var map: ShardMap = initial
        private set

    val self: NodeId get() = membership.self

    fun start(scope: CoroutineScope) {
        source.watch().onEach { map = it }.launchIn(scope)
    }
}

/**
 * The façade the server calls to resolve ownership. It holds TWO independent rings:
 *
 * - the **room** ring — owns write coordination + locker/lock/subscription state, keyed by
 *   `(LockerKeyspace, roomId)`;
 * - the **session (gateway)** ring — owns the live WebSocket + inbox delivery for a session,
 *   keyed by `sessionId` under [Keyspace.SESSION].
 *
 * Each ring has its own [Membership], [ShardMapSource], epoch, and shard counts, so the
 * session/gateway tier can be a separate pool of WebSocket servers that scales independently of
 * the room tier (or the same fleet, via [coLocated] — that is a deployment choice, not a
 * routing one). A room write therefore fans out across session-owning nodes by the session ring.
 */
class ShardRouter private constructor(
    private val room: RingView,
    private val session: RingView,
    private val locator: PeerLocator,
) {
    /** This node's identity on the room ring. */
    val roomSelf: NodeId get() = room.self

    /** This node's identity on the session/gateway ring. */
    val sessionSelf: NodeId get() = session.self

    val roomEpoch: Epoch get() = room.map.epoch
    val sessionEpoch: Epoch get() = session.map.epoch

    fun roomMap(): ShardMap = room.map
    fun sessionMap(): ShardMap = session.map

    fun start(scope: CoroutineScope) {
        room.start(scope)
        session.start(scope)
    }

    /** Resolve the room ring: who coordinates writes/fan-out for `(keyspace, roomId)`. */
    suspend fun routeRoom(keyspace: Keyspace, roomId: ByteArray): Route =
        resolve(room, room.map.owner(keyspace, roomId))

    /** Resolve the session ring: which WebSocket/gateway server hosts this session. */
    suspend fun routeSession(sessionId: ByteArray): Route =
        resolve(session, session.map.owner(Keyspace.SESSION, sessionId))

    private suspend fun resolve(ring: RingView, owner: NodeId): Route {
        val isLocal = owner == ring.self
        return Route(
            isLocal = isLocal,
            node = owner,
            address = if (isLocal) null else locator.addressOf(owner),
            epoch = ring.map.epoch,
        )
    }

    companion object {
        /** Wire the room and session rings from independent sources (the general case). */
        suspend fun create(
            roomMembership: Membership,
            roomSource: ShardMapSource,
            sessionMembership: Membership,
            sessionSource: ShardMapSource,
            locator: PeerLocator,
            scope: CoroutineScope,
        ): ShardRouter {
            val router = ShardRouter(
                room = RingView(roomMembership, roomSource, roomSource.current()),
                session = RingView(sessionMembership, sessionSource, sessionSource.current()),
                locator = locator,
            )
            router.start(scope)
            return router
        }

        /**
         * Both axes on one ring/membership/source (single-fleet deployment). Rooms and sessions
         * still route independently via their distinct keyspaces, but share a node set and epoch.
         */
        suspend fun coLocated(
            membership: Membership,
            source: ShardMapSource,
            locator: PeerLocator,
            scope: CoroutineScope,
        ): ShardRouter = create(membership, source, membership, source, locator, scope)
    }
}
