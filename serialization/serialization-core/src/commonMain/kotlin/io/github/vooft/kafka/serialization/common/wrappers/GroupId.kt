package io.github.vooft.kafka.serialization.common.wrappers

import io.github.vooft.kafka.serialization.common.KafkaString
import io.github.vooft.kafka.serialization.common.primitives.IntEncoding
import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

@KafkaString(lengthEncoding = IntEncoding.INT16)
@Serializable
@JvmInline
value class GroupId(val value: String)