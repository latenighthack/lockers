package com.latenighthack.lockers.connector.test

import com.latenighthack.ktcrypto.*
import com.latenighthack.lockers.common.LockerSigning
import com.latenighthack.lockers.common.v1.*
import com.latenighthack.lockers.connector.ecdsaDerToRaw
import kotlinx.coroutines.runBlocking
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertTrue

class LockSigningRoundTripTest {
    @Test
    fun `sign and verify writeContext round trips`() = runBlocking {
        val kp = Secp256r1KeyPair.generate()
        val roomId = RoomId(Random.nextBytes(32))
        val lockerId = LockerId(Random.nextBytes(32), LockerKeyspace(1))
        val hash = SHA256.digest("payload".encodeToByteArray())

        val ctx = LockerSigning.writeContext(roomId, lockerId, 0L, hash)
        // Mirrors the client: convert the DER signature to the raw form ktcrypto's verify expects.
        val sig = ecdsaDerToRaw(kp.privateKey.sign(ctx))

        val decoded = Secp256r1PublicKey.decode(kp.publicKey.encode())
        assertTrue(decoded.verify(ctx, sig), "decoded public key should verify the signature")
    }
}
