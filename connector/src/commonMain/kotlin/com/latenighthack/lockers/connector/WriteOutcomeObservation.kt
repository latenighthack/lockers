package com.latenighthack.lockers.connector
import com.latenighthack.lockers.room.v1.*

/** Immutable observations of the retained receipt. Responses contain metadata, never source payloads. */
sealed interface WriteOutcomeObservation {
    val terminal: Boolean
    class Response(response: GetWriteOutcomeResponse) : WriteOutcomeObservation {
        private val bytes = response.toByteArray()
        val response: GetWriteOutcomeResponse get() = GetWriteOutcomeResponse.fromByteArray(bytes.copyOf())
        override val terminal = response.result is GetWriteOutcomeResponse.Result.INVALID || response.result.isOk() && when (response.outcome?.agentState) {
            is WriteOutcome.AgentState.APPLIED, is WriteOutcome.AgentState.FAILED, is WriteOutcome.AgentState.INDETERMINATE -> true
            else -> false
        }
        override fun equals(other: Any?) = other is Response && bytes.contentEquals(other.bytes)
        override fun hashCode() = bytes.contentHashCode()
    }
    enum class UnavailableReason { NOT_SUPPORTED, DEADLINE_EXCEEDED }
    class Unavailable(val reason: UnavailableReason, lastResponse: GetWriteOutcomeResponse? = null) : WriteOutcomeObservation {
        private val bytes = lastResponse?.toByteArray()
        val lastResponse: GetWriteOutcomeResponse? get() = bytes?.let { GetWriteOutcomeResponse.fromByteArray(it.copyOf()) }
        override val terminal = true
    }
}
