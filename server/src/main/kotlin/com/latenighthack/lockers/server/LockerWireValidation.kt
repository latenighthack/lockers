package com.latenighthack.lockers.server

/** Validate lengths before the generated decoder can allocate or pad truncated submessages. */
internal object LockerWireValidation {
    private enum class Shape { LOCKER, OPEN, SEALED, PAYLOAD, ENCLOSURE, SIGNATURE, SHARED_KEY, KEY }
    fun valid(bytes: ByteArray): Boolean = bytes.size <= ProtocolValidation.MAX_ENVELOPE_BYTES &&
        scan(bytes, 0, bytes.size, Shape.LOCKER)

    private fun scan(bytes: ByteArray, start: Int, end: Int, shape: Shape): Boolean {
        var position = start
        fun varint(): Long? {
            var result = 0L
            for (i in 0..9) {
                if (position >= end) return null
                val value = bytes[position++].toInt() and 255
                if (i == 9 && value > 1) return null
                result = result or ((value and 127).toLong() shl (i * 7))
                if (value and 128 == 0) return result
            }
            return null
        }
        while (position < end) {
            val tag = varint() ?: return false
            if (tag <= 0 || tag > 0xffffffffL || tag ushr 3 == 0L) return false
            val field = (tag ushr 3).toInt()
            val wire = (tag and 7).toInt()
            val child = when (shape) {
                Shape.LOCKER -> when (field) { 1 -> Shape.OPEN; 2 -> Shape.SEALED; else -> null }
                Shape.SEALED -> if (field == 2) Shape.PAYLOAD else null
                Shape.PAYLOAD -> when (field) { 1 -> Shape.KEY; 3 -> Shape.ENCLOSURE; 5 -> Shape.SHARED_KEY; else -> null }
                Shape.ENCLOSURE -> if (field == 1) Shape.SIGNATURE else null
                Shape.SIGNATURE, Shape.SHARED_KEY -> if (field == 1) Shape.KEY else null
                else -> null
            }
            val knownBytes = when (shape) {
                Shape.OPEN, Shape.KEY -> field == 1
                Shape.PAYLOAD -> field == 2 || field == 4
                Shape.ENCLOSURE, Shape.SIGNATURE, Shape.SHARED_KEY -> field == 2
                else -> false
            }
            if ((child != null || knownBytes) && wire != 2) return false
            when (wire) {
                0 -> if (varint() == null) return false
                1 -> { if (end - position < 8) return false; position += 8 }
                2 -> {
                    val length = varint() ?: return false
                    if (length < 0 || length > end - position) return false
                    val limit = position + length.toInt()
                    if (child != null && !scan(bytes, position, limit, child)) return false
                    position = limit
                }
                5 -> { if (end - position < 4) return false; position += 4 }
                else -> return false
            }
        }
        return position == end
    }
}
