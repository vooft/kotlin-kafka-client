package io.github.vooft.kafka.cluster

import io.github.vooft.kafka.common.types.KafkaTopic
import io.github.vooft.kafka.common.types.NodeId
import io.github.vooft.kafka.common.types.PartitionIndex

interface KafkaTopicStateProvider {
    val topic: KafkaTopic
    suspend fun topicPartitions(): Map<PartitionIndex, NodeId>
}
