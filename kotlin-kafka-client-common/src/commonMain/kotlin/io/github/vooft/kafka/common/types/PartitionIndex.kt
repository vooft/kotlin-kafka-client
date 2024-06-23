package io.github.vooft.kafka.common.types

import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

@Serializable
@JvmInline
value class PartitionIndex(val value: Int)
