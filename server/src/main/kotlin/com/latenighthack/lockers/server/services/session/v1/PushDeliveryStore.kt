package com.latenighthack.lockers.server.services.session.v1

import com.latenighthack.ktstore.StoreDelegate
import com.latenighthack.lockers.server.services.room.v1.DeliveryOutboxStore

/** Separate durable queue so a slow push gateway cannot hold up the next room event. */
class PushDeliveryStore(delegate: StoreDelegate) {
    val outbox = DeliveryOutboxStore(delegate, "push_delivery")
}
