package io.github.vooft.kafka.common

import io.github.vooft.kafka.serialization.common.IntValue
import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

@Serializable
@JvmInline
value class PartitionIndex(override val value: Int): IntValue
