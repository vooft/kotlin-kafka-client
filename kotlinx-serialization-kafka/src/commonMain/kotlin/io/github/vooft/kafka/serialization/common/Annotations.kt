package io.github.vooft.kafka.serialization.common

import kotlinx.serialization.SerialInfo

@SerialInfo
@Target(AnnotationTarget.PROPERTY)
annotation class KafkaCrc32Prefixed

@SerialInfo
@Target(AnnotationTarget.PROPERTY)
annotation class KafkaSizeInBytesPrefixed(val encoding: IntEncoding = IntEncoding.INT32)

@SerialInfo
@Target(AnnotationTarget.PROPERTY)
annotation class KafkaCollectionWithVarIntSize

@SerialInfo
@Target(AnnotationTarget.PROPERTY)
annotation class KafkaSkipCollectionSize

@SerialInfo
@Target(AnnotationTarget.CLASS)
annotation class KafkaString(val encoding: IntEncoding)

enum class IntEncoding {
    INT16,
    INT32,
    VARINT
}
