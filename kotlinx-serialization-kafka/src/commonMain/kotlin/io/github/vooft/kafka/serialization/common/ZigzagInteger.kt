package io.github.vooft.kafka.serialization.common

object ZigzagInteger {
    fun encode(value: Int): Int {
        return (value shl 1) xor (value shr 31)
    }

    fun decode(value: Int): Int {
        return (value ushr 1) xor -(value and 1)
    }

    fun encode(value: Long): Long {
        return (value shl 1) xor (value shr 63)
    }

    fun decode(value: Long): Long {
        return (value ushr 1) xor -(value and 1)
    }
}
