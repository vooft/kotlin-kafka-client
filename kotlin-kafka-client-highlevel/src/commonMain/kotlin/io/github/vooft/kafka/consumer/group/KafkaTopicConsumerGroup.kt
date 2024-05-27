package io.github.vooft.kafka.consumer.group

import io.github.vooft.kafka.cluster.KafkaConnectionPool
import io.github.vooft.kafka.cluster.KafkaMetadataManager
import io.github.vooft.kafka.cluster.TopicMetadataProvider
import io.github.vooft.kafka.common.NodeId
import io.github.vooft.kafka.network.common.toInt16String
import io.github.vooft.kafka.network.messages.FindCoordinatorRequestV1
import io.github.vooft.kafka.network.messages.FindCoordinatorResponseV1
import io.github.vooft.kafka.network.messages.JoinGroupRequestV1
import io.github.vooft.kafka.network.messages.JoinGroupResponseV1
import io.github.vooft.kafka.network.sendRequest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class KafkaTopicConsumerGroup(
    private val topic: String,
    private val groupId: String,
    private val metadataManager: KafkaMetadataManager,
    private val connectionPool: KafkaConnectionPool,
    private val coroutineScope: CoroutineScope = CoroutineScope(Job())
) {

    private val topicMetadataProviderDeferred = coroutineScope.async(start = CoroutineStart.LAZY) {
        metadataManager.topicMetadataProvider(topic)
    }

    private val memberMetadata = mutableMapOf<String, MutableStateFlow<GroupMemberMetadata>>()
    private val memberMetadataMutex = Mutex()

    suspend fun createGroupedTopicMetadataProvider(): TopicMetadataProvider {
        val newGroupMemberMetadata = joinGroup()
        val flow = MutableStateFlow(newGroupMemberMetadata)
        memberMetadataMutex.withLock {
            memberMetadata[newGroupMemberMetadata.memberId] = flow
        }

        val topicMetadataProvider = topicMetadataProviderDeferred.await()
        return GroupedTopicMetadataProvider(
            topicMetadataProvider = topicMetadataProvider,
            groupMemberMetadataFlow = flow,
            assignedPartitionsFlow = MutableStateFlow(emptyList()), // TODO: add after SyncGroup is implemented
            coroutineScope = coroutineScope
        )
    }

    private suspend fun joinGroup(memberId: String = ""): GroupMemberMetadata {
        val coordinatorNodeId = findCoordinator()
        val connection = connectionPool.acquire(coordinatorNodeId)

        val response = connection.sendRequest<JoinGroupRequestV1, JoinGroupResponseV1>(
            JoinGroupRequestV1(
                groupId = groupId.toInt16String(),
                sessionTimeoutMs = 30000,
                rebalanceTimeoutMs = 10000,
                memberId = memberId.toInt16String(),
                protocolType = CONSUMER_PROTOCOL_TYPE.toInt16String(),
                groupProtocols = listOf(
                    JoinGroupRequestV1.GroupProtocol(
                        name = "mybla".toInt16String(), // TODO: change to proper assigner
                        metadata = JoinGroupRequestV1.GroupProtocol.Metadata(
                            topics = listOf(topic.toInt16String())
                        )
                    )
                )
            )
        )

        return GroupMemberMetadata(
            memberId = response.memberId.nonNullValue,
            isLeader = response.memberId == response.leaderId,
            generationId = response.generationId
        )
    }

    private suspend fun findCoordinator(): NodeId {
        val connection = connectionPool.acquire()
        val response = connection.sendRequest<FindCoordinatorRequestV1, FindCoordinatorResponseV1>(
            FindCoordinatorRequestV1(groupId.toInt16String())
        )

        return response.nodeId
    }
}

data class GroupMemberMetadata(val memberId: String, val isLeader: Boolean, val generationId: Int)

// looks like in kafka itself they use "consumer" for this use case
private const val CONSUMER_PROTOCOL_TYPE = "consumer"
