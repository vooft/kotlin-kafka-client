package io.github.vooft.kafka.common

import io.github.vooft.kafka.serialization.common.IntEncoding
import io.github.vooft.kafka.serialization.common.KafkaString
import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

@KafkaString(encoding = IntEncoding.INT16)
@Serializable
@JvmInline
value class MemberId(val value: String)
