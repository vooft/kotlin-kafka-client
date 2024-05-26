package io.github.vooft.kafka.producer

import io.github.vooft.kafka.common.BrokerAddress
import io.github.vooft.kafka.common.NodeId
import io.github.vooft.kafka.common.PartitionIndex
import io.github.vooft.kafka.network.KafkaConnection
import io.github.vooft.kafka.network.messages.ErrorCode
import io.github.vooft.kafka.network.messages.MetadataRequestV1
import io.github.vooft.kafka.network.messages.MetadataResponseV1
import io.github.vooft.kafka.network.sendRequest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

interface KafkaMetadataManager {
    suspend fun queryLatestNodes(): Map<NodeId, BrokerAddress>
    suspend fun queryTopicMetadata(topic: String): TopicMetadata
    suspend fun registerTopic(topic: String)
    suspend fun removeTopic(topic: String)
}

class KafkaMetadataManagerImpl(
    private val bootstrapConnections: List<Deferred<KafkaConnection>>,
    private val coroutineScope: CoroutineScope = CoroutineScope(Job())
) : KafkaMetadataManager {

    private val topicsMetadata = mutableMapOf<String, Deferred<TopicMetadata>>()
    private val topicsMetadataMutex = Mutex()

    private val nodes = mutableMapOf<NodeId, BrokerAddress>()
    private val nodesMutex = Mutex()

    // TODO: launch background thread to refresh metadatas

    override suspend fun queryLatestNodes(): Map<NodeId, BrokerAddress> {
        require(nodes.isNotEmpty()) { "Nodes are not initialized, please query at least one topic" }
        return nodesMutex.withLock { nodes.toMap() }
    }

    override suspend fun queryTopicMetadata(topic: String): TopicMetadata {
        // TODO: create thread-safe mutable map wrapper
        if (!topicsMetadata.containsKey(topic)) {
            registerTopic(topic)
        }

        return topicsMetadataMutex.withLock { topicsMetadata.getValue(topic) }.await()
    }

    override suspend fun registerTopic(topic: String) {
        topicsMetadataMutex.withLock {
            if (!topicsMetadata.containsKey(topic)) {
                topicsMetadata[topic] = coroutineScope.async { querySingleTopicMetadata(topic) }
            }
        }
    }

    override suspend fun removeTopic(topic: String) {
        val topicMetadata = topicsMetadataMutex.withLock { topicsMetadata.remove(topic) }
        topicMetadata?.cancel()
    }

    private suspend fun queryTopicsMetadataAndUpdateBrokers(topics: Collection<String>): Map<String, TopicMetadata>? {
        val connection = bootstrapConnections.random().await()
        val response = connection.sendRequest<MetadataRequestV1, MetadataResponseV1>(MetadataRequestV1(topics))
        println("metadata for topics $topics")
        println(response)

        if (response.topics.any { it.errorCode == ErrorCode.UNKNOWN_TOPIC_OR_PARTITION || it.errorCode == ErrorCode.LEADER_NOT_AVAILABLE }) {
            return null
        }

        val newNodes = response.brokers.associate { it.nodeId to BrokerAddress(it.host.nonNullValue, it.port) }
        if (newNodes != nodes) {
            nodesMutex.withLock {
                nodes.clear()
                nodes.putAll(newNodes)
            }
        }

        return response.topics.associate { topic ->
            topic.name.nonNullValue to TopicMetadata(
                topic = topic.name.nonNullValue,
                partitions = topic.partitions.associate { it.partition to it.leader }
            )
        }
    }

    private suspend fun querySingleTopicMetadata(topic: String): TopicMetadata {
        var metadata: Map<String, TopicMetadata>? = null
        while (metadata == null) {
            metadata = queryTopicsMetadataAndUpdateBrokers(listOf(topic))
        }

        return metadata.getValue(topic)
    }
}

data class TopicMetadata(val topic: String, val partitions: Map<PartitionIndex, NodeId>)


