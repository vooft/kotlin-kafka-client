package io.github.vooft.kafka.consumer.requests

import io.github.vooft.kafka.network.messages.FetchRequestV4
import io.github.vooft.kafka.serialization.common.primitives.int32ListOf
import io.github.vooft.kafka.serialization.common.primitives.toInt32List
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex
import io.github.vooft.kafka.serialization.common.wrappers.PartitionOffset

object ConsumerRequestsFactory {
    fun fetchRequest(topic: KafkaTopic, partitionOffsets: Map<PartitionIndex, PartitionOffset>) = FetchRequestV4(
        maxWaitTime = 500,
        minBytes = 1,
        maxBytes = 1024 * 1024,
        topics = int32ListOf(
            FetchRequestV4.Topic(
                topic = topic,
                partitions = partitionOffsets.map { (index, offset) ->
                    FetchRequestV4.Topic.Partition(
                        partition = index,
                        fetchOffset = offset,
                        maxBytes = 1024 * 1024
                    )
                }.toInt32List()
            )
        )
    )
}
