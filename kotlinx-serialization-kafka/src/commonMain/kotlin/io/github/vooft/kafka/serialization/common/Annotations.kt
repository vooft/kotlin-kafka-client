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

enum class IntEncoding {
    INT32,
    VARINT
}
