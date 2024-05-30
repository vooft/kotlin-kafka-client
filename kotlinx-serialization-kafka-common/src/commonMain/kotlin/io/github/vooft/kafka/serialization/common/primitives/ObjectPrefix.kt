package io.github.vooft.kafka.serialization.common.primitives

import io.github.vooft.kafka.serialization.common.IntEncoding.INT32
import io.github.vooft.kafka.serialization.common.IntEncoding.VARINT
import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

@KafkaBytesSizePrefixed(sizeEncoding = VARINT)
@Serializable
@JvmInline
value class VarIntBytesSizePrefixed<T>(val value: T)


@KafkaBytesSizePrefixed(sizeEncoding = INT32)
@Serializable
@JvmInline
value class Int32BytesSizePrefixed<T>(val value: T)
