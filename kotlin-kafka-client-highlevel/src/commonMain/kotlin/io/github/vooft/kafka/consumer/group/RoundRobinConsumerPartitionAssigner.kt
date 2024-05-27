package io.github.vooft.kafka.consumer.group

import io.github.vooft.kafka.common.PartitionIndex

object RoundRobinConsumerPartitionAssigner {
    fun assign(partitions: List<PartitionIndex>, members: List<String>): Map<String, List<PartitionIndex>> {
        val result = mutableMapOf<String, List<PartitionIndex>>()
        for ((index, partition) in partitions.withIndex()) {
            val member = members[index % members.size]
            val existing = result[member] ?: emptyList()
            result[member] = existing + partition
        }

        for (member in members) {
            result[member] = result[member] ?: emptyList()
        }

        return result.toMap()
    }
}
