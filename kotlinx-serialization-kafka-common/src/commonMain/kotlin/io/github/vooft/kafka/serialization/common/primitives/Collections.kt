package io.github.vooft.kafka.serialization.common.primitives

import io.github.vooft.kafka.serialization.common.IntEncoding
import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

@KafkaCollection(sizeEncoding = IntEncoding.VARINT)
@Serializable
@JvmInline
value class VarIntList<T>(val value: List<T>): Iterable<T> by value

@KafkaCollection(sizeEncoding = IntEncoding.INT32)
@Serializable
@JvmInline
value class Int32List<T>(val value: List<T>): Iterable<T> by value {
    constructor(vararg values: T): this(values.toList())
}
