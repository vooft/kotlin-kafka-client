package io.github.vooft.kafka.consumer.requests

import io.github.vooft.kafka.common.PartitionIndex
import io.github.vooft.kafka.network.common.toInt16String
import io.github.vooft.kafka.network.messages.FetchRequestV4

object ConsumerRequestsFactory {
    fun fetchRequest(topic: String, partitions: List<PartitionIndex>) = FetchRequestV4(
        maxWaitTime = 500,
        minBytes = 1,
        maxBytes = 1024 * 1024,
        topics = listOf(
            FetchRequestV4.Topic(
                topic = topic.toInt16String(),
                partitions = partitions.map {
                    FetchRequestV4.Topic.Partition(
                        partition = it,
                        fetchOffset = 0,
                        maxBytes = 1024 * 1024
                    )
                }
            )
        )
    )
}
