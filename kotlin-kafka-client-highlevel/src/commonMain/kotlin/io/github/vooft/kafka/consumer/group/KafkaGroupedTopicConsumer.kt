package io.github.vooft.kafka.consumer.group

import io.github.vooft.kafka.cluster.KafkaConnectionPool
import io.github.vooft.kafka.cluster.KafkaTopicStateProvider
import io.github.vooft.kafka.common.GroupId
import io.github.vooft.kafka.common.KafkaTopic
import io.github.vooft.kafka.common.MemberId
import io.github.vooft.kafka.common.NodeId
import io.github.vooft.kafka.common.PartitionIndex
import io.github.vooft.kafka.consumer.KafkaTopicConsumer
import io.github.vooft.kafka.consumer.SimpleKafkaTopicConsumer
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
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.launch

class KafkaGroupedTopicConsumer(
    override val topic: KafkaTopic,
    private val groupId: GroupId,
    private val topicStateProvider: KafkaTopicStateProvider,
    private val connectionPool: KafkaConnectionPool,
    private val coroutineScope: CoroutineScope
) : KafkaTopicConsumer {

    private val heartbeatJob = coroutineScope.launch {
        sendHeartbeatInfinite()
    }

    private val consumerMetadata = coroutineScope.async {
        val metadata = rejoinGroup(MemberId.EMPTY)
        MutableStateFlow(metadata)
    }

    private val delegate = SimpleKafkaTopicConsumer(GroupedTopicMetadataProvider(), connectionPool, coroutineScope)

    override suspend fun consume() = delegate.consume()

    private suspend fun rejoinGroup(memberId: MemberId): ExtendedConsumerMetadata {
        val coordinator = findCoordinator()

        val joinedGroup = joinGroup(coordinator, memberId)
        val assignedPartitions = syncGroup(joinedGroup, joinedGroup.otherMembers)
        return ExtendedConsumerMetadata(
            membership = joinedGroup,
            assignedPartitions = assignedPartitions,
        )
    }

    private suspend fun sendHeartbeatInfinite() {
        while (true) {
            delay(5000)

            val metadata = consumerMetadata.await()
            when (val errorCode = sendHeartbeat()) {
                NO_ERROR -> Unit
                REBALANCE_IN_PROGRESS, NOT_COORDINATOR, ILLEGAL_GENERATION -> metadata.value = rejoinGroup(metadata.value.membership.memberId)
                UNKNOWN_MEMBER_ID -> metadata.value = rejoinGroup(MemberId.EMPTY)
                else -> error("Heartbeat failed with error code $errorCode")
            }
        }
    }

    private suspend fun sendHeartbeat(): ErrorCode {
        val metadata = consumerMetadata.await().value

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

    private suspend fun syncGroup(joinedGroup: JoinedGroup, memberIds: Collection<MemberId>): List<PartitionIndex> {
        val partitions = topicStateProvider.topicPartitions()

        val assignments = when (joinedGroup.isLeader) {
            true -> RoundRobinConsumerPartitionAssigner.assign(
                partitions = partitions.keys.toList(),
                members = memberIds.toList()
            )

            false -> mapOf()
        }

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

    inner class GroupedTopicMetadataProvider : KafkaTopicStateProvider {
        override val topic: KafkaTopic get() = this@KafkaGroupedTopicConsumer.topic
        override suspend fun topicPartitions(): Map<PartitionIndex, NodeId> {
            val assignedPartitions = consumerMetadata.await().value.assignedPartitions
            return topicStateProvider.topicPartitions().filterKeys { it in assignedPartitions }
        }
    }
}
