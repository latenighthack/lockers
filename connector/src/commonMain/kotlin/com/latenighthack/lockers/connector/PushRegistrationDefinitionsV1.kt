package com.latenighthack.lockers.connector
import com.latenighthack.ktstore.*
import com.latenighthack.ktbuf.net.RpcClient
import com.latenighthack.lockers.common.v1.SessionId
import com.latenighthack.lockers.connector.storage.v1.StoredPushRegistration
import com.latenighthack.lockers.connector.storage.v1.fromByteArray
import com.latenighthack.lockers.connector.storage.v1.toByteArray
import com.latenighthack.lockers.push.v1.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch

private val StoredPushRegistration.backendKeyStorage: ByteArray get() = intToBytes(requireNotNull(backend))

object PushRegistrationStoreImplDefinitionV1 : StoreDefinition<StoredPushRegistration>(
    StoreName("push_registrations"), "StoredPushRegistration-protobuf-v1", StoredPushRegistration.Companion::fromByteArray, StoredPushRegistration::toByteArray,
) {
    val backendKey = bytesIndex(IndexName("backendintToBytes"), StoredPushRegistration::backendKeyStorage, "intToBytes-v1").also { primaryKey(it) }
}

internal fun intToBytes(value: Int): ByteArray = byteArrayOf(
    (value ushr 24).toByte(),
    (value ushr 16).toByte(),
    (value ushr 8).toByte(),
    value.toByte(),
)
