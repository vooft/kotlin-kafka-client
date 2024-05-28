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
import io.github.vooft.kafka.network.messages.ErrorCode
import io.github.vooft.kafka.network.messages.ErrorCode.ILLEGAL_GENERATION
import io.github.vooft.kafka.network.messages.ErrorCode.NOT_COORDINATOR
import io.github.vooft.kafka.network.messages.ErrorCode.NO_ERROR
import io.github.vooft.kafka.network.messages.ErrorCode.REBALANCE_IN_PROGRESS
import io.github.vooft.kafka.network.messages.ErrorCode.UNKNOWN_MEMBER_ID
import io.github.vooft.kafka.network.messages.FindCoordinatorRequestV1
import io.github.vooft.kafka.network.messages.FindCoordinatorResponseV1
import io.github.vooft.kafka.network.messages.HeartbeatRequestV0
import io.github.vooft.kafka.network.messages.HeartbeatResponseV0
import io.github.vooft.kafka.network.messages.JoinGroupRequestV1
import io.github.vooft.kafka.network.messages.JoinGroupResponseV1
import io.github.vooft.kafka.network.messages.MemberAssignment
import io.github.vooft.kafka.network.messages.SyncGroupRequestV1
import io.github.vooft.kafka.network.messages.SyncGroupResponseV1
import io.github.vooft.kafka.network.sendRequest
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart.LAZY
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.jvm.JvmInline

class KafkaConsumerGroupManager(
    private val topic: KafkaTopic,
    private val groupId: GroupId,
    private val metadataManager: KafkaMetadataManager,
    private val connectionPool: KafkaConnectionPool,
    private val coroutineScope: CoroutineScope = CoroutineScope(Job())
) {

    private val topicMetadataProviderDeferred = coroutineScope.async(start = LAZY) {
        metadataManager.topicMetadataProvider(topic)
    }

    private val consumerMetadataFlow = mutableMapOf<InternalConsumerId, MutableStateFlow<ExtendedConsumerMetadata>>()
    private val consumerMetadataMutex = Mutex()

    suspend fun nextGroupedTopicMetadataProvider(): TopicMetadataProvider {
        val consumerId = InternalConsumerId.next()
        val consumerMetadata = rejoinGroup(consumerId, MemberId.EMPTY)

        val flow = MutableStateFlow(consumerMetadata)
        consumerMetadataMutex.withLock {
            require(!consumerMetadataFlow.containsKey(consumerId)) { "Consumer with id $consumerId already exists" }
            consumerMetadataFlow[consumerId] = flow
        }

        consumerMetadata.heartbeatJob.start()

        val topicMetadataProvider = topicMetadataProviderDeferred.await()
        return GroupedTopicMetadataProvider(
            topicMetadataProvider = topicMetadataProvider,
            consumerMetadataFlow = flow
        )
    }

    private suspend fun joinGroup(coordinatorNodeId: NodeId, memberId: MemberId = MemberId("")): JoinedGroup {
        val connection = connectionPool.acquire(coordinatorNodeId)

        val response = connection.sendRequest<JoinGroupRequestV1, JoinGroupResponseV1>(
            JoinGroupRequestV1(
                groupId = groupId,
                sessionTimeoutMs = 30000,
                rebalanceTimeoutMs = 60000,
                memberId = memberId,
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

        return JoinedGroup(
            coordinatorNodeId = coordinatorNodeId,
            memberId = response.memberId,
            isLeader = response.leaderId == response.memberId,
            generationId = response.generationId,
            otherMembers = response.members.map { it.memberId }
        )
    }

    private suspend fun sendHeartbeatInfinite(consumerId: InternalConsumerId) {
        while (true) {
            delay(5000)

            val metadata = consumerMetadataMutex.withLock { consumerMetadataFlow[consumerId]?.value }
            if (metadata == null) {
                val newMetadata = rejoinGroup(consumerId, MemberId.EMPTY)
                consumerMetadataMutex.withLock { consumerMetadataFlow[consumerId] = MutableStateFlow(newMetadata) }
            } else {
                when (val errorCode = sendHeartbeat(metadata)) {
                    NO_ERROR -> Unit
                    REBALANCE_IN_PROGRESS, NOT_COORDINATOR, ILLEGAL_GENERATION -> rejoinGroup(consumerId, metadata.membership.memberId)
                    UNKNOWN_MEMBER_ID -> consumerMetadataMutex.withLock { consumerMetadataFlow.remove(consumerId) }
                    else -> error("Heartbeat failed with error code $errorCode")
                }
            }
        }
    }

    suspend fun sendHeartbeat(metadata: ConsumerMetadata): ErrorCode {
        val connection = connectionPool.acquire(metadata.membership.coordinatorNodeId)
        val response = connection.sendRequest<HeartbeatRequestV0, HeartbeatResponseV0>(
            HeartbeatRequestV0(
                groupId = groupId,
                generationId = metadata.membership.generationId,
                memberId = metadata.membership.memberId
            )
        )

        return response.errorCode
    }

    private suspend fun rejoinGroup(consumerId: InternalConsumerId, memberId: MemberId): ExtendedConsumerMetadata {
        val coordinator = findCoordinator()

        val joinedGroup = joinGroup(coordinator, memberId)
        val assignedPartitions = syncGroup(joinedGroup, joinedGroup.otherMembers)
        return ExtendedConsumerMetadata(
            membership = joinedGroup,
            assignedPartitions = assignedPartitions,
            heartbeatJob = coroutineScope.launch(start = LAZY) { sendHeartbeatInfinite(consumerId) }
        )
    }

    private suspend fun syncGroup(joinedGroup: JoinedGroup, memberIds: Collection<MemberId>): List<PartitionIndex> {
        println("syncGroup")
        val topicMetadata = topicMetadataProviderDeferred.await().topicMetadata()
        println("syncGroup: topicMetadata $topicMetadata")

        val assignments = when (joinedGroup.isLeader) {
            true -> RoundRobinConsumerPartitionAssigner.assign(
                partitions = topicMetadata.partitions.keys.toList(),
                members = memberIds.toList()
            )

            false -> mapOf()
        }
        println("assigned partitions: $assignments")

        val connection = connectionPool.acquire(joinedGroup.coordinatorNodeId)
        val syncResponse = connection.sendRequest<SyncGroupRequestV1, SyncGroupResponseV1>(SyncGroupRequestV1(
            groupId = groupId,
            generationId = joinedGroup.generationId,
            memberId = joinedGroup.memberId,
            assignments = assignments.map { (memberId, partitions) ->
                SyncGroupRequestV1.Assignment(
                    memberId = memberId,
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
        private val consumerMetadataFlow: StateFlow<ConsumerMetadata>,
    ) : TopicMetadataProvider {

        override val topic: KafkaTopic get() = topicMetadataProvider.topic

        override suspend fun topicMetadata(): TopicMetadata {
            val consumerMetadata = consumerMetadataFlow.value
            sendHeartbeat(consumerMetadata)

            val topicMetadata = topicMetadataProvider.topicMetadata()
            val assignedPartitions = consumerMetadata.assignedPartitions

            return topicMetadata.copy(partitions = topicMetadata.partitions.filterKeys { it in assignedPartitions })
        }
    }
}

data class ExtendedConsumerMetadata(
    override val membership: JoinedGroup,
    override val assignedPartitions: List<PartitionIndex>,
    val heartbeatJob: Job
) : ConsumerMetadata

interface ConsumerMetadata {
    val membership: ConsumerGroupMembership
    val assignedPartitions: List<PartitionIndex>
}

interface ConsumerGroupMembership {
    val coordinatorNodeId: NodeId
    val memberId: MemberId
    val isLeader: Boolean
    val generationId: Int
}

data class JoinedGroup(
    override val coordinatorNodeId: NodeId,
    override val memberId: MemberId,
    override val isLeader: Boolean,
    override val generationId: Int,
    val otherMembers: List<MemberId>
) : ConsumerGroupMembership

@JvmInline
value class InternalConsumerId private constructor(val id: Int) {
    companion object {
        private var counter = 0
        fun next() = InternalConsumerId(counter++)
    }
}

// looks like in kafka itself they use "consumer" for this use case
private const val CONSUMER_PROTOCOL_TYPE = "consumer"
