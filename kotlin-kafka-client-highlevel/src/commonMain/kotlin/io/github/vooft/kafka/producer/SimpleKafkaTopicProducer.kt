package io.github.vooft.kafka.producer

import io.github.vooft.kafka.cluster.KafkaConnectionPool
import io.github.vooft.kafka.cluster.TopicMetadata
import io.github.vooft.kafka.cluster.TopicMetadataProvider
import io.github.vooft.kafka.common.KafkaTopic
import io.github.vooft.kafka.common.PartitionIndex
import io.github.vooft.kafka.network.messages.ProduceRequestV3
import io.github.vooft.kafka.network.messages.ProduceResponseV3
import io.github.vooft.kafka.network.sendRequest
import io.github.vooft.kafka.producer.requests.ProduceRequestFactory
import io.github.vooft.kafka.producer.requests.ProducedRecord
import kotlinx.io.Source
import kotlinx.io.readByteArray

class SimpleKafkaTopicProducer(
    override val topic: KafkaTopic,
    private val topicMetadataProvider: TopicMetadataProvider,
    private val connectionPool: KafkaConnectionPool
) : KafkaTopicProducer {

    override suspend fun send(key: Source, value: Source): RecordMetadata {
        // TODO: add support for custom-provided partition
        val topicMetadata = topicMetadataProvider.topicMetadata()
        val partition = topicMetadata.determinePartition(key)
        val node = topicMetadata.partitions.getValue(partition)
        val connection = connectionPool.acquire(node)

        val request = ProduceRequestFactory.createProduceRequest(topic, partition, listOf(ProducedRecord(key, value)))
        val response = connection.sendRequest<ProduceRequestV3, ProduceResponseV3>(request)

        return RecordMetadata(
            topic = response.topicResponses.single().topic,
            partition = response.topicResponses.single().partitionResponses.single().index,
            errorCode = response.topicResponses.single().partitionResponses.single().errorCode
        )
    }

    private fun TopicMetadata.determinePartition(key: Source): PartitionIndex {
        // TODO: use murmur
        val partitionCount = partitions.size
        println("partition count $partitionCount")
        val keyBytes = key.peek().readByteArray()
        val keyHash = keyBytes.fold(0) { acc, byte -> acc + byte.toInt() }
        return PartitionIndex(keyHash % partitionCount)
    }
}
