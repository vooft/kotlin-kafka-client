package io.github.vooft.kafka.serialization.common.primitives

import io.github.vooft.kafka.serialization.common.KafkaCollection
import io.github.vooft.kafka.serialization.common.primitives.IntEncoding.INT32
import io.github.vooft.kafka.serialization.common.primitives.IntEncoding.VARINT
import kotlinx.io.Buffer
import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

@KafkaCollection(sizeEncoding = VARINT)
@Serializable
@JvmInline
value class VarIntList<T>(val value: List<T>): Iterable<T> by value

@KafkaCollection(sizeEncoding = INT32)
@Serializable
@JvmInline
value class Int32List<T>(val value: List<T>): Iterable<T> by value {
    constructor(vararg values: T): this(values.toList())

    companion object {
        fun <T> empty(): Int32List<T> = Int32List(emptyList())
    }
}

fun <T> List<T>.toInt32List() = Int32List(this)
fun <T> int32ListOf() = Int32List<T>(emptyList())
fun <T> int32ListOf(vararg values: T): Int32List<T> = Int32List(values.toList())

// TODO: find a shortcut for just copying byte array from source, instead of reading byte-by-byte
@Serializable
@KafkaCollection(sizeEncoding = VARINT)
@JvmInline
// TODO: replace with ByteArray once figure out how to make comparision work
// https://youtrack.jetbrains.com/issue/KT-24874/Support-custom-equals-and-hashCode-for-value-classes
value class VarIntByteArray(val data: List<Byte>) {
    constructor(data: ByteArray): this(data.toList())
}

fun VarIntByteArray(value: String) = VarIntByteArray(value.encodeToByteArray())
fun VarIntByteArray.toBuffer() = Buffer().apply { write(data.toByteArray()) }

@Serializable
@KafkaCollection(sizeEncoding = INT32)
@JvmInline
value class Int32ByteArray(val data: List<Byte>) {
    constructor(data: ByteArray): this(data.toList())
}

fun Int32ByteArray(value: String) = Int32ByteArray(value.encodeToByteArray())
fun Int32ByteArray.toBuffer() = Buffer().apply { write(data.toByteArray()) }
