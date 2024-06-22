package io.github.vooft.kafka.serialization.common

import kotlinx.io.Source
import kotlinx.io.readByteArray
import kotlin.jvm.JvmInline

// CRC32C implementation, adapted from https://github.com/caffeine-mgn/pw.binom.io
object CRC32 {
    fun crc32c(buffer: Source): Int {
        return CRC32C_TABLE.compute(buffer)
    }
}

// maybe will use in future
//private const val CRC32_POLY: UInt = 0xEDB88320u
private const val CRC32C_POLY: UInt = 0x82F63B78u

private val CRC32C_TABLE = CRC32Table(CRC32C_POLY)

@JvmInline
private value class CRC32Table(val table: IntArray) {
    constructor(poly: UInt) : this(IntArray(256) { n ->
        var c = n
        repeat(8) {
            c = if (c and 1 != 0) {
                poly.toInt() xor (c ushr 1)
            } else {
                c ushr 1
            }
        }
        c
    })
}

private fun CRC32Table.applyByte(byte: Byte, c: Int): Int {
    val o = byte.toInt() and 0xFF
    return (c ushr 8) xor table[o xor (c and 0xff)]
}

private fun CRC32Table.compute(source: Source): Int {
    var crc = 0.inv()

    for (byte in source.readByteArray()) {
        crc = applyByte(byte, crc)
    }

    return crc.inv()
}
