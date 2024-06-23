package io.github.vooft.kafka.cluster

import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.NodeId
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex

interface KafkaTopicStateProvider {
    val topic: KafkaTopic
    suspend fun topicPartitions(): Map<PartitionIndex, NodeId>
}
