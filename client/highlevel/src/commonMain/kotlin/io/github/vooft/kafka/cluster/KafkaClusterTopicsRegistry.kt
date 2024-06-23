package io.github.vooft.kafka.cluster

import io.github.vooft.kafka.network.common.ErrorCode.LEADER_NOT_AVAILABLE
import io.github.vooft.kafka.network.common.ErrorCode.NO_ERROR
import io.github.vooft.kafka.network.common.ErrorCode.UNKNOWN_TOPIC_ID
import io.github.vooft.kafka.network.common.ErrorCode.UNKNOWN_TOPIC_OR_PARTITION
import io.github.vooft.kafka.network.metadata
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.NodeId
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant

interface KafkaClusterTopicsRegistry {
    suspend fun topicPartitions(topic: KafkaTopic): Map<PartitionIndex, NodeId>
}

fun KafkaClusterTopicsRegistry.forTopic(topic: KafkaTopic): KafkaTopicStateProvider = object : KafkaTopicStateProvider {
    override val topic: KafkaTopic = topic
    override suspend fun topicPartitions(): Map<PartitionIndex, NodeId> = topicPartitions(topic)
}

class KafkaClusterTopicsRegistryImpl(
    private val connectionPool: KafkaConnectionPool,
) : KafkaClusterTopicsRegistry {

    private val topicsState = mutableMapOf<KafkaTopic, MutableStateFlow<TopicState>>()
    private val topicsStateMutex = Mutex()

    // TODO: add background job to refresh

    override suspend fun topicPartitions(topic: KafkaTopic): Map<PartitionIndex, NodeId> {
        val existingFlow = topicsStateMutex.withLock { topicsState[topic] }
        if (existingFlow != null) {
            // TODO: check for expiry
            return existingFlow.value.partitionLayout
        } else {
            val existingTopics = topicsStateMutex.withLock { topicsState.keys.toList() }
            updateMetadata(existingTopics + topic)
        }

        return topicsStateMutex.withLock {
            // TODO: there is a race condition and sometimes topicState doesn't contain topic
            topicsState.getValue(topic).value.partitionLayout
        }
    }

    private suspend fun updateMetadata(topics: Collection<KafkaTopic>) {
        val newMetadata = refreshMetadata(topics)
        for ((topic, metadata) in newMetadata) {
            topicsStateMutex.withLock {
                topicsState.getOrPut(topic) { MutableStateFlow(metadata) }.value = metadata
            }
        }
    }

    private suspend fun refreshMetadata(topics: Collection<KafkaTopic>): Map<KafkaTopic, TopicState> {
        val connection = connectionPool.acquire()

        val now = Clock.System.now()

        val response = connection.metadata(topics)
        return buildMap {
            for (topic in response.topics) {
                when (topic.errorCode) {
                    NO_ERROR -> topicsStateMutex.withLock {
                        put(
                            topic.topic, TopicState(
                                updated = now,
                                partitionLayout = topic.partitions.associateBy({ it.partition }, { it.leader })
                            )
                        )
                    }

                    UNKNOWN_TOPIC_ID, UNKNOWN_TOPIC_OR_PARTITION -> {
                        // TODO: fail?
                    }

                    LEADER_NOT_AVAILABLE -> putAll(refreshMetadata(topics)) // TODO: retry properly

                    else -> error("Failed to fetch topic metadata for ${topic.topic} with error code ${topic.errorCode}")
                }
            }
        }
    }
}

private data class TopicState(val updated: Instant, val partitionLayout: Map<PartitionIndex, NodeId>)
