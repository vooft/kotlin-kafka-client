package io.github.vooft.kafka.consumer.requests

import io.github.vooft.kafka.network.messages.FetchRequestV4
import io.github.vooft.kafka.serialization.common.primitives.int32ListOf
import io.github.vooft.kafka.serialization.common.primitives.toInt32List
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex

object ConsumerRequestsFactory {
    fun fetchRequest(topic: KafkaTopic, partitions: List<PartitionIndex>) = FetchRequestV4(
        maxWaitTime = 500,
        minBytes = 1,
        maxBytes = 1024 * 1024,
        topics = int32ListOf(
            FetchRequestV4.Topic(
                topic = topic,
                partitions = partitions.map {
                    FetchRequestV4.Topic.Partition(
                        partition = it,
                        fetchOffset = 0,
                        maxBytes = 1024 * 1024
                    )
                }.toInt32List()
            )
        )
    )
}
