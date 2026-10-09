package com.latenighthack.lockers.server.tools

import io.micrometer.core.instrument.MeterRegistry
import kotlin.reflect.KProperty
import com.latenighthack.lockers.observability.*

suspend fun <T, U : com.latenighthack.ktbuf.proto.Enum> MeterRegistry.trackResponse(key: String, resultProp: KProperty<U>, telemetry: LockersTelemetry = LockersTelemetry.NONE, callback: suspend () -> T): T {
    val operation = when (key) {
        "lockers.room.locker.subscribe" -> TelemetryOperation.ROOM_SUBSCRIPTION
        "lockers.room.locker.get" -> TelemetryOperation.ROOM_GET
        "lockers.room.locker.getall" -> TelemetryOperation.ROOM_GET_ALL
        "lockers.room.locker.postlockerchange" -> TelemetryOperation.ROOM_WRITE
        "lockers.room.locker.deletelocker" -> TelemetryOperation.ROOM_DELETE
        "lockers.room.locker.lock" -> TelemetryOperation.ROOM_LOCK
        "lockers.room.locker.unlock" -> TelemetryOperation.ROOM_UNLOCK
        "lockers.session.post" -> TelemetryOperation.SESSION_POST
        "lockers.session.broadcast" -> TelemetryOperation.SESSION_BROADCAST
        "lockers.push.register" -> TelemetryOperation.PUSH_REGISTER
        "lockers.push.unregister" -> TelemetryOperation.PUSH_UNREGISTER
        "lockers.push.config" -> TelemetryOperation.PUSH_CONFIG
        "lockers.push.sendpush" -> TelemetryOperation.PUSH_ENQUEUE
        else -> null
    }
    val response = if (operation == null) callback() else trackRpc(operation, telemetry, { rpcOutcome(resultProp.getter.call(it).toString()) }, callback)
    val responseEnum = resultProp.getter.call(response)
    val responseString = responseEnum.toString()

    counter(key, "result", responseString).increment()

    return response
}
