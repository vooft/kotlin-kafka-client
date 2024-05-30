package io.github.vooft.kafka.producer

import io.github.vooft.kafka.cluster.KafkaConnectionPool
import io.github.vooft.kafka.cluster.KafkaTopicStateProvider
import io.github.vooft.kafka.network.messages.ProduceRequestV3
import io.github.vooft.kafka.network.messages.ProduceResponseV3
import io.github.vooft.kafka.network.sendRequest
import io.github.vooft.kafka.producer.requests.ProduceRequestFactory
import io.github.vooft.kafka.producer.requests.ProducedRecord
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.NodeId
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex
import kotlinx.io.Source
import kotlinx.io.readByteArray

class SimpleKafkaTopicProducer(
    override val topic: KafkaTopic,
    private val topicStateProvider: KafkaTopicStateProvider,
    private val connectionPool: KafkaConnectionPool
) : KafkaTopicProducer {

    override suspend fun send(key: Source, value: Source): RecordMetadata {
        // TODO: add support for custom-provided partition
        val (partition, node) = topicStateProvider.determinePartition(key)
        val connection = connectionPool.acquire(node)

        val request = ProduceRequestFactory.createProduceRequest(topic, partition, listOf(ProducedRecord(key, value)))
        val response = connection.sendRequest<ProduceRequestV3, ProduceResponseV3>(request)

        return RecordMetadata(
            topic = response.topics.single().topic,
            partition = response.topics.single().partitions.single().index,
            errorCode = response.topics.single().partitions.single().errorCode
        )
    }

    private suspend fun KafkaTopicStateProvider.determinePartition(key: Source): Pair<PartitionIndex, NodeId> {
        // TODO: use murmur
        val partitionToNodeMap = topicPartitions()
        val partitions = partitionToNodeMap.keys.sortedBy { it.value }

        val keyBytes = key.peek().readByteArray()
        val keyHash = keyBytes.fold(0) { acc, byte -> acc + byte.toInt() }

        val selectedPartition = partitions[keyHash % partitions.size]
        return selectedPartition to partitionToNodeMap.getValue(selectedPartition)
    }
}
