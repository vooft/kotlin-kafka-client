package io.github.vooft.kafka.serialization.common.customtypes

import io.github.vooft.kafka.serialization.common.IntEncoding
import io.github.vooft.kafka.serialization.common.KafkaString
import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

@KafkaString(encoding = IntEncoding.VARINT)
@Serializable
@JvmInline
value class VarIntString(val value: String?): KafkaCustomType

