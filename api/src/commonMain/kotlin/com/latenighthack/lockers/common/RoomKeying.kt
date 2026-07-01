package com.latenighthack.lockers.common

import com.latenighthack.lockers.common.v1.RoomId

/**
 * Encoding for "public-keyed" rooms, where the room's authority key is carried in the
 * room id itself. The key is suffixed with a marker byte so plain opaque room ids
 * (typically 32 random bytes) are never mistaken for public-keyed ones:
 *
 *   public-keyed id = <33-byte compressed secp256r1 public key> ‖ MARKER   (34 bytes)
 *   anything else   = opaque room (TOFU)
 *
 * Detection is structural (length + marker) here; callers still decode the extracted
 * bytes to confirm they form a valid point before trusting them as an authority key.
 */
object RoomKeying {
    const val PUBLIC_KEYED_MARKER: Byte = 0x6B
    private const val COMPRESSED_KEY_SIZE = 33
    private const val PUBLIC_KEYED_ID_SIZE = COMPRESSED_KEY_SIZE + 1

    /** Build a public-keyed room id from a 33-byte compressed secp256r1 public key. */
    fun publicKeyed(compressedPublicKey: ByteArray): RoomId {
        require(compressedPublicKey.size == COMPRESSED_KEY_SIZE) {
            "expected a 33-byte compressed secp256r1 public key, got ${compressedPublicKey.size}"
        }
        return RoomId(rawValue = compressedPublicKey + PUBLIC_KEYED_MARKER)
    }

    /** The embedded authority key bytes if [roomId] is shaped as public-keyed, else null. */
    fun authorityKey(roomId: RoomId): ByteArray? {
        val raw = roomId.rawValue
        if (raw.size != PUBLIC_KEYED_ID_SIZE) return null
        if (raw[raw.size - 1] != PUBLIC_KEYED_MARKER) return null
        return raw.copyOfRange(0, COMPRESSED_KEY_SIZE)
    }
}
