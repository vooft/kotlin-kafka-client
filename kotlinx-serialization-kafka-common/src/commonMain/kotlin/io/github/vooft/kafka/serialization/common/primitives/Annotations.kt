@file:OptIn(ExperimentalSerializationApi::class)

package io.github.vooft.kafka.serialization.common.primitives

import io.github.vooft.kafka.serialization.common.IntEncoding
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerialInfo

@SerialInfo
@Target(AnnotationTarget.CLASS)
annotation class KafkaCollection(val sizeEncoding: IntEncoding)

@SerialInfo
@Target(AnnotationTarget.CLASS)
annotation class KafkaBytesSizePrefixed(val sizeEncoding: IntEncoding)

@SerialInfo
@Target(AnnotationTarget.CLASS)
annotation class KafkaString(val lengthEncoding: IntEncoding)

