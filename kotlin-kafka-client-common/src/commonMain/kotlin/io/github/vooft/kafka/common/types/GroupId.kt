package io.github.vooft.kafka.common.types

import io.github.vooft.kafka.common.annotations.IntEncoding
import io.github.vooft.kafka.common.annotations.KafkaString
import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

@KafkaString(lengthEncoding = IntEncoding.INT16)
@Serializable
@JvmInline
value class GroupId(val value: String)
