package io.github.vooft.kafka.serialization.common

annotation class KafkaCrc32Prefixed

@Target(AnnotationTarget.PROPERTY)
annotation class KafkaByteSizePrefixed

@Target(AnnotationTarget.PROPERTY)
annotation class KafkaSkipCollectionSize
