package com.latenighthack.lockers.connector

/** Exhaustion rejects acceptance before ACK; no unconsumed application event is silently dropped. */
data class ConnectorRetentionPolicy(val maxAcceptedEvents: Long = 100_000, val maxAcknowledgements: Long = 100_000, val maxAcceptedEventBytes: Long = 64 * 1024 * 1024, val maxCachedLockers: Long = 100_000) {
    init { require(maxAcceptedEvents > 0 && maxAcknowledgements > 0 && maxAcceptedEventBytes > 0 && maxCachedLockers > 0) }
}
class ConnectorRetentionExceededException(message: String) : IllegalStateException(message)
class ConnectorCursorExpiredException(val cursor: Long, val retainedAfter: Long) : IllegalStateException("Cursor $cursor precedes retained cursor $retainedAfter")
