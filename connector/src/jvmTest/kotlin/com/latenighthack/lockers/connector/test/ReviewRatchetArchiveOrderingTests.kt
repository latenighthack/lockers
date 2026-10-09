package com.latenighthack.lockers.connector.test

import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.*
import com.latenighthack.lockers.connector.internal.*
import com.latenighthack.lockers.room.v1.*
import kotlinx.coroutines.runBlocking
import kotlin.test.*

class ReviewRatchetArchiveOrderingTests {
    private suspend fun archive(epoch: Long, target: Byte, sourceVersion: Long): ArchivedRatchet {
        val key = Secp256r1KeyPair.generate()
        val pub = Secp256R1Key.PublicKey(key.publicKey.encode())
        return ArchivedRatchet(PendingRatchet(PostLockerChangeRequest(roomId = RoomId(byteArrayOf(1)),
            lockerId = LockerId(byteArrayOf(target), LockerKeyspace(0)), writeRequestId = ByteArray(32) { target },
            ratchet = PostLockerChangeRequest.Ratchet(newPublicKey = pub)), key.privateKey.encode()),
            LockState(locked = true, scope = LockScope(kind = LockScopeKind.LOCK_SCOPE_ROOM), lockVersion = epoch, publicKey = pub), sourceVersion)
    }
    @Test fun `late older receipt cannot replace a newer private archive in a shared scope`() = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open(); val store = LockerStoreImpl(db)
        try {
            val older = archive(2, 2, 999); val newer = archive(3, 3, 1)
            store.archiveRatchet(newer); store.archiveRatchet(older)
            assertContentEquals(newer.publicKey, store.archivedRatchets().single().publicKey)
            assertEquals(3L, store.archivedRatchets().single().state!!.lockVersion)
        } finally { db.close() }
    }
    @Test fun `stale obsolete archive cleanup cannot delete its newer scope replacement`() = runBlocking {
        val db = ConnectorStorage.inMemory(); db.open(); val store = LockerStoreImpl(db)
        try {
            val older = archive(2, 2, 999); val newer = archive(3, 3, 1)
            store.archiveRatchet(older); store.archiveRatchet(newer)
            store.forgetArchivedRatchet(older)
            assertContentEquals(newer.publicKey, store.archivedRatchets().single().publicKey)
            store.forgetArchivedRatchet(newer); assertTrue(store.archivedRatchets().isEmpty())
        } finally { db.close() }
    }
}
