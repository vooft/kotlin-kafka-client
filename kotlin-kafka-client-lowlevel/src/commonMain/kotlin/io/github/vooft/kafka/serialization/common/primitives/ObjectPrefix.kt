package io.github.vooft.kafka.serialization.common.primitives

import io.github.vooft.kafka.common.annotations.IntEncoding.INT32
import io.github.vooft.kafka.common.annotations.IntEncoding.VARINT
import io.github.vooft.kafka.common.annotations.KafkaBytesSizePrefixed
import io.github.vooft.kafka.common.annotations.KafkaCrc32cPrefixed
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

@KafkaCrc32cPrefixed
@Serializable
@JvmInline
value class Crc32cPrefixed<T>(val value: T)
