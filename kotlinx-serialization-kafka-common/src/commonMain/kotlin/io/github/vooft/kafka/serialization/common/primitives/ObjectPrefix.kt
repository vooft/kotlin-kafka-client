package io.github.vooft.kafka.serialization.common.primitives

import io.github.vooft.kafka.serialization.common.IntEncoding.VARINT
import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

@KafkaBytesSizePrefixed(sizeEncoding = VARINT)
@Serializable
@JvmInline
value class VarIntBytesSizePrefixed<T>(val value: T)
