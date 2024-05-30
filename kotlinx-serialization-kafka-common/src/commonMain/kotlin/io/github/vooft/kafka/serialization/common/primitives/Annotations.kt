@file:OptIn(ExperimentalSerializationApi::class)

package io.github.vooft.kafka.serialization.common.primitives

import io.github.vooft.kafka.serialization.common.IntEncoding
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerialInfo
import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

@SerialInfo
@Target(AnnotationTarget.CLASS)
annotation class KafkaCollection(val sizeEncoding: IntEncoding)

@SerialInfo
@Target(AnnotationTarget.CLASS)
annotation class KafkaString(val lengthEncoding: IntEncoding)

@KafkaCollection(sizeEncoding = IntEncoding.VARINT)
@Serializable
@JvmInline
value class VarIntCollection<T>(val value: List<T>)

@KafkaCollection(sizeEncoding = IntEncoding.INT32)
@Serializable
@JvmInline
value class Int32Collection<T>(val value: List<T>)
