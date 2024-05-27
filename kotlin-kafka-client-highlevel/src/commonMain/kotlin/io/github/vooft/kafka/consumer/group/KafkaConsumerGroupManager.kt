package io.github.vooft.kafka.consumer.group

import io.github.vooft.kafka.cluster.KafkaConnectionPool
import io.github.vooft.kafka.cluster.KafkaMetadataManager
import io.github.vooft.kafka.cluster.TopicMetadata
import io.github.vooft.kafka.cluster.TopicMetadataProvider
import io.github.vooft.kafka.common.GroupId
import io.github.vooft.kafka.common.KafkaTopic
import io.github.vooft.kafka.common.MemberId
import io.github.vooft.kafka.common.NodeId
import io.github.vooft.kafka.common.PartitionIndex
import io.github.vooft.kafka.network.common.toInt16String
import io.github.vooft.kafka.network.messages.FindCoordinatorRequestV1
import io.github.vooft.kafka.network.messages.FindCoordinatorResponseV1
import io.github.vooft.kafka.network.messages.JoinGroupRequestV1
import io.github.vooft.kafka.network.messages.JoinGroupResponseV1
import io.github.vooft.kafka.network.messages.MemberAssignment
import io.github.vooft.kafka.network.messages.SyncGroupRequestV1
import io.github.vooft.kafka.network.messages.SyncGroupResponseV1
import io.github.vooft.kafka.network.sendRequest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class KafkaConsumerGroupManager(
    private val topic: KafkaTopic,
    private val groupId: GroupId,
    private val metadataManager: KafkaMetadataManager,
    private val connectionPool: KafkaConnectionPool,
    private val coroutineScope: CoroutineScope = CoroutineScope(Job())
) {

    private val topicMetadataProviderDeferred = coroutineScope.async(start = CoroutineStart.LAZY) {
        metadataManager.topicMetadataProvider(topic)
    }

    private val memberMetadata = mutableMapOf<String, MutableStateFlow<GroupMemberMetadata>>()
    private val memberMetadataMutex = Mutex()

    private val assignedPartitionsFlows = mutableMapOf<String, MutableStateFlow<List<PartitionIndex>>>()
    private val assignedPartitionsMutex = Mutex()

    suspend fun nextGroupedTopicMetadataProvider(): TopicMetadataProvider {
        topicMetadataProviderDeferred.await().topicMetadata()

        val coordinatorNodeId = findCoordinator()
        println("found coordinator $coordinatorNodeId")

        val newGroupMemberMetadata = joinGroup(coordinatorNodeId)
        val memberMetadataFlow = MutableStateFlow(newGroupMemberMetadata)
        memberMetadataMutex.withLock {
            memberMetadata[newGroupMemberMetadata.memberId] = memberMetadataFlow
        }

        val assignedPartitions = syncGroup(
            coordinatorNodeId = coordinatorNodeId,
            current = newGroupMemberMetadata,
            memberIds = memberMetadata.keys.toList()
        )
        val assignedPartitionsFlow = MutableStateFlow(assignedPartitions)
        assignedPartitionsMutex.withLock {
            assignedPartitionsFlows[newGroupMemberMetadata.memberId] = assignedPartitionsFlow
        }

        val topicMetadataProvider = topicMetadataProviderDeferred.await()
        return GroupedTopicMetadataProvider(
            topicMetadataProvider = topicMetadataProvider,
            groupMemberMetadataFlow = memberMetadataFlow.asStateFlow(),
            assignedPartitionsFlow = assignedPartitionsFlow.asStateFlow()
        )
    }

    private suspend fun joinGroup(coordinatorNodeId: NodeId, memberId: String = ""): GroupMemberMetadata {
        val connection = connectionPool.acquire(coordinatorNodeId)

        val response = connection.sendRequest<JoinGroupRequestV1, JoinGroupResponseV1>(
            JoinGroupRequestV1(
                groupId = groupId,
                sessionTimeoutMs = 30000,
                rebalanceTimeoutMs = 60000,
                memberId = MemberId(memberId),
                protocolType = CONSUMER_PROTOCOL_TYPE.toInt16String(),
                groupProtocols = listOf(
                    JoinGroupRequestV1.GroupProtocol(
                        protocol = "mybla".toInt16String(), // TODO: change to proper assigner
                        metadata = JoinGroupRequestV1.GroupProtocol.Metadata(
                            topics = listOf(topic)
                        )
                    )
                )
            )
        )

        return GroupMemberMetadata(
            memberId = response.memberId.value,
            isLeader = response.memberId == response.leaderId,
            generationId = response.generationId
        )
    }

    suspend fun sendHeartbeat(memberMetadata: GroupMemberMetadata) {

    }

    private suspend fun syncGroup(coordinatorNodeId: NodeId, current: GroupMemberMetadata, memberIds: List<String>): List<PartitionIndex> {
        println("syncGroup")
        val topicMetadata = topicMetadataProviderDeferred.await().topicMetadata()
        println("syncGroup: topicMetadata $topicMetadata")

        val assignments = when (current.isLeader) {
            true -> RoundRobinConsumerPartitionAssigner.assign(
                partitions = topicMetadata.partitions.keys.toList(),
                members = memberIds
            )
            false -> mapOf()
        }
        println("assignd partitons: $assignments")

        val connection = connectionPool.acquire(coordinatorNodeId)
        val syncResponse = connection.sendRequest<SyncGroupRequestV1, SyncGroupResponseV1>(SyncGroupRequestV1(
            groupId = groupId,
            generationId = current.generationId,
            memberId = MemberId(current.memberId),
            assignments = assignments.map { (memberId, partitions) ->
                SyncGroupRequestV1.Assignment(
                    memberId = MemberId(memberId),
                    assignment = MemberAssignment(
                        partitionAssignments = listOf(
                            MemberAssignment.PartitionAssignment(
                                topic = topic,
                                partitions = partitions
                            )
                        ),
                    )
                )
            }
        ))

        return syncResponse.assignment?.partitionAssignments?.single()?.partitions ?: listOf()

    }

    private suspend fun findCoordinator(): NodeId {
        while (true) {
            val connection = connectionPool.acquire()
            val response = connection.sendRequest<FindCoordinatorRequestV1, FindCoordinatorResponseV1>(
                FindCoordinatorRequestV1(groupId.value.toInt16String()) // TODO: create 2 types: one for groups, one for txns
            )

            if (response.errorCode.isRetriable) {
                continue
            }

            return response.nodeId
        }
    }

    private inner class GroupedTopicMetadataProvider(
        private val topicMetadataProvider: TopicMetadataProvider,
        private val groupMemberMetadataFlow: StateFlow<GroupMemberMetadata>,
        private val assignedPartitionsFlow: StateFlow<List<PartitionIndex>>,
    ) : TopicMetadataProvider {

        // TODO: launch heartbeat

        override val topic: KafkaTopic get() = topicMetadataProvider.topic

        override suspend fun topicMetadata(): TopicMetadata {
            sendHeartbeat(groupMemberMetadataFlow.value)

            val topicMetadata = topicMetadataProvider.topicMetadata()
            val assignedPartitions = assignedPartitionsFlow.value

            return topicMetadata.copy(partitions = topicMetadata.partitions.filterKeys { it in assignedPartitions })
        }
    }
}

data class GroupMemberMetadata(val memberId: String, val isLeader: Boolean, val generationId: Int)

// looks like in kafka itself they use "consumer" for this use case
private const val CONSUMER_PROTOCOL_TYPE = "consumer"
