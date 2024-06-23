package io.github.vooft.kafka.common.types

import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

@Serializable
@JvmInline
value class PartitionOffset(val value: Long): Comparable<PartitionOffset> {
    operator fun plus(other: Int) = plus(other.toLong())
    operator fun plus(other: Long) = plus(PartitionOffset(other))
    operator fun plus(other: PartitionOffset) = PartitionOffset(value + other.value)
    override fun compareTo(other: PartitionOffset): Int = value.compareTo(other.value)
}
