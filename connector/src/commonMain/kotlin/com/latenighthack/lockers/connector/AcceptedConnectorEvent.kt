package com.latenighthack.lockers.connector
import com.latenighthack.lockers.common.v1.Event
/** Every application acceptance cursor, including variants a particular consumer chooses to ignore. */
sealed interface AcceptedConnectorEvent {
    val cursor: Long
    data class LockerChanged(override val cursor: Long, val change: LockerClient.LockerUpdate) : AcceptedConnectorEvent
    data class SessionEvent(override val cursor: Long, val event: Event) : AcceptedConnectorEvent
}
data class AcceptedNotification(val cursor: Long, val notification: IncomingNotification)
