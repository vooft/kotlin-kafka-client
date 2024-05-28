@file:OptIn(ExperimentalSerializationApi::class)

package io.github.vooft.kafka.serialization.common

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerialInfo

@SerialInfo
@Target(AnnotationTarget.PROPERTY)
annotation class KafkaCrc32Prefixed

@SerialInfo
@Target(AnnotationTarget.PROPERTY)
annotation class KafkaSizeInBytesPrefixed(val encoding: IntEncoding)

@SerialInfo
@Target(AnnotationTarget.PROPERTY)
annotation class KafkaCollectionWithVarIntSize

@SerialInfo
@Target(AnnotationTarget.CLASS)
annotation class KafkaString(val encoding: IntEncoding)

enum class IntEncoding {
    INT16,
    INT32,
    VARINT
}
