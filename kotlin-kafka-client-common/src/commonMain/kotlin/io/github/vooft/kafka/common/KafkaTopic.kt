package io.github.vooft.kafka.common

import io.github.vooft.kafka.serialization.common.IntEncoding
import io.github.vooft.kafka.serialization.common.primitives.KafkaString
import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

@KafkaString(lengthEncoding = IntEncoding.INT16)
@Serializable
@JvmInline
value class KafkaTopic(val value: String)
