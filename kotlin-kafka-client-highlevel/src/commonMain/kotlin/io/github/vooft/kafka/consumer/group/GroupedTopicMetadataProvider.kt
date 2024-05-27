package io.github.vooft.kafka.consumer.group

import io.github.vooft.kafka.cluster.TopicMetadata
import io.github.vooft.kafka.cluster.TopicMetadataProvider
import io.github.vooft.kafka.common.PartitionIndex
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.StateFlow

class GroupedTopicMetadataProvider(
    private val topicMetadataProvider: TopicMetadataProvider,
    private val groupMemberMetadataFlow: StateFlow<GroupMemberMetadata>,
    private val assignedPartitionsFlow: StateFlow<List<PartitionIndex>>,
    private val coroutineScope: CoroutineScope = CoroutineScope(Job())
) : TopicMetadataProvider {

    // TODO: launch heartbeat

    override val topic: String get() = topicMetadataProvider.topic

    override suspend fun topicMetadata(): TopicMetadata {
        val topicMetadata = topicMetadataProvider.topicMetadata()
        val assignedPartitions = assignedPartitionsFlow.value

        return topicMetadata.copy(partitions = topicMetadata.partitions.filterKeys { it in assignedPartitions })
    }
}
