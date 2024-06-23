@file:OptIn(ExperimentalSerializationApi::class)

package io.github.vooft.kafka.common.annotations

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
annotation class KafkaCrc32cPrefixed

@SerialInfo
@Target(AnnotationTarget.CLASS)
annotation class KafkaString(val lengthEncoding: IntEncoding)

