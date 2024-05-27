package io.github.vooft.kafka.cluster

import io.github.vooft.kafka.common.BrokerAddress
import io.github.vooft.kafka.common.NodeId
import io.github.vooft.kafka.common.PartitionIndex
import io.github.vooft.kafka.network.KafkaConnection
import io.github.vooft.kafka.network.messages.MetadataRequestV1
import io.github.vooft.kafka.network.messages.MetadataResponseV1
import io.github.vooft.kafka.network.sendRequest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.concurrent.Volatile

interface KafkaMetadataManager {
    fun nodesProvider(): NodesProvider
    suspend fun topicMetadataProvider(topic: String): TopicMetadataProvider
}

interface NodesProvider {
    suspend fun nodes(): Nodes
}

interface TopicMetadataProvider {
    val topic: String
    suspend fun topicMetadata(): TopicMetadata
}

typealias Nodes = Map<NodeId, BrokerAddress>

// TODO: launch background thread to refresh metadatas
class KafkaMetadataManagerImpl(
    private val bootstrapConnections: List<Deferred<KafkaConnection>>,
    private val coroutineScope: CoroutineScope = CoroutineScope(Job())
) : KafkaMetadataManager {

    @Volatile
    private var topicsMetadata = emptyMap<String, MutableSharedFlow<TopicMetadata>>()
    private val topicsMetadataMutex = Mutex()

    private val nodes = MutableSharedFlow<Map<NodeId, BrokerAddress>>(replay = 1)

    init {
        coroutineScope.launch {
            val metadata = queryMetadata(listOf())
            nodes.emit(metadata.nodes)
        }
    }

    // TODO: add checking for metadata expiration and force-update if needed
    override fun nodesProvider(): NodesProvider = object : NodesProvider {
        override suspend fun nodes(): Nodes = nodes.first()
    }

    override suspend fun topicMetadataProvider(topic: String): TopicMetadataProvider {
        val flow = getOrCreateTopicsMetadataFlow(topic)
        return object : TopicMetadataProvider {
            override val topic: String = topic
            override suspend fun topicMetadata(): TopicMetadata = flow.first()
        }
    }

    private suspend fun getOrCreateTopicsMetadataFlow(topic: String): MutableSharedFlow<TopicMetadata> {
        val firstExisting = topicsMetadata[topic]
        if (firstExisting != null) {
            return firstExisting
        }

        return topicsMetadataMutex.withLock {
            val secondExisting = topicsMetadata[topic]
            if (secondExisting != null) {
                return@withLock secondExisting
            }

            val newFlow = MutableSharedFlow<TopicMetadata>(replay = 1)
            topicsMetadata = topicsMetadata + (topic to newFlow)
            newFlow
        }.also { coroutineScope.launch { refreshMetadata() } }
    }

    private suspend fun refreshMetadata() {
        println("refreshMetadata")
        val topics = topicsMetadata

        val metadata = queryMetadata(topics.keys)
        nodes.emit(metadata.nodes)

        topics.forEach { (topic, flow) ->
            flow.emit(metadata.topics.getValue(topic))
        }
    }

    private suspend fun queryMetadata(topics: Collection<String>): TopicAndNodesMetadata {
        val response = queryMetadataRetryable(topics)

        val newNodes = response.brokers.associate { it.nodeId to BrokerAddress(it.host.nonNullValue, it.port) }
        val newTopics = response.topics.associate { topic ->
            topic.name.nonNullValue to TopicMetadata(
                topic = topic.name.nonNullValue,
                partitions = topic.partitions.associate { it.partition to it.leader }
            )
        }

        return TopicAndNodesMetadata(newNodes, newTopics)
    }

    // TODO: make more generic
    private suspend fun queryMetadataRetryable(topics: Collection<String>): MetadataResponseV1 {
        val request = MetadataRequestV1(topics)
        while (true) {
            val connection = bootstrapConnections.random().await()
            val response = connection.sendRequest<MetadataRequestV1, MetadataResponseV1>(request)
            if (response.topics.none { it.errorCode.isRetriable }) {
                return response
            }

            println("retrying due to ${response.topics.first { it.errorCode.isRetriable }}")
            delay(100)
        }
    }

    private suspend fun querySingleTopicMetadata(topic: String): TopicMetadata {
        val metadata = queryMetadata(listOf(topic))
        return metadata.topics.getValue(topic)
    }
}

data class TopicMetadata(val topic: String, val partitions: Map<PartitionIndex, NodeId>)
data class TopicAndNodesMetadata(val nodes: Nodes, val topics: Map<String, TopicMetadata>)


