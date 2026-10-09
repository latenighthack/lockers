package com.latenighthack.lockers.server.services.session.v1

import com.latenighthack.lockers.server.tools.QueueMetrics
import com.latenighthack.ktstore.Database
import com.latenighthack.lockers.server.services.room.v1.DeliveryOutboxStore

/** Separate durable queue so a slow push gateway cannot hold up the next room event. */
class PushDeliveryStore(delegate: Database, metrics: QueueMetrics? = null) {
    val outbox = DeliveryOutboxStore(delegate, "push_delivery", metrics = metrics)
}
