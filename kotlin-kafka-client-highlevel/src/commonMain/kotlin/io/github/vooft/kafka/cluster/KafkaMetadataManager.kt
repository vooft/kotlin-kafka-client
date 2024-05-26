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
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
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
    private var topicsMetadata = emptyMap<String, MutableStateFlow<TopicMetadata>>()
    private val topicsMetadataMutex = Mutex()

    private val mutableNodesFlow = MutableSharedFlow<Map<NodeId, BrokerAddress>>(replay = 1)

    // TODO: add checking for metadata expiration and force-update if needed
    override fun nodesProvider(): NodesProvider = object : NodesProvider {
        override suspend fun nodes(): Nodes = mutableNodesFlow.first()
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
            coroutineScope.launch { newFlow.emit(querySingleTopicMetadata(topic)) }
            return@withLock newFlow
        }
    }

    private suspend fun queryTopicsMetadataAndUpdateBrokers(topics: Collection<String>): Map<String, TopicMetadata> {
        val response = queryMetadataRetryable(topics)

        val newNodes = response.brokers.associate { it.nodeId to BrokerAddress(it.host.nonNullValue, it.port) }
        mutableNodesFlow.emit(newNodes)

        return response.topics.associate { topic ->
            topic.name.nonNullValue to TopicMetadata(
                topic = topic.name.nonNullValue,
                partitions = topic.partitions.associate { it.partition to it.leader }
            )
        }
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
        }
    }

    private suspend fun querySingleTopicMetadata(topic: String): TopicMetadata {
        val metadata = queryTopicsMetadataAndUpdateBrokers(listOf(topic))
        return metadata.getValue(topic)
    }
}

data class TopicMetadata(val topic: String, val partitions: Map<PartitionIndex, NodeId>)


