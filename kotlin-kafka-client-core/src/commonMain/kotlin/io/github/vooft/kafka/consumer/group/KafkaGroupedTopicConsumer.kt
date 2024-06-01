package io.github.vooft.kafka.consumer.group

import io.github.vooft.kafka.cluster.KafkaConnectionPool
import io.github.vooft.kafka.cluster.KafkaTopicStateProvider
import io.github.vooft.kafka.consumer.KafkaTopicConsumer
import io.github.vooft.kafka.consumer.SimpleKafkaTopicConsumer
import io.github.vooft.kafka.consumer.offset.ConsumerGroupOffsetProvider
import io.github.vooft.kafka.network.common.ErrorCode
import io.github.vooft.kafka.network.common.ErrorCode.ILLEGAL_GENERATION
import io.github.vooft.kafka.network.common.ErrorCode.NOT_COORDINATOR
import io.github.vooft.kafka.network.common.ErrorCode.NO_ERROR
import io.github.vooft.kafka.network.common.ErrorCode.REBALANCE_IN_PROGRESS
import io.github.vooft.kafka.network.common.ErrorCode.UNKNOWN_MEMBER_ID
import io.github.vooft.kafka.network.common.toInt16String
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
import io.github.vooft.kafka.serialization.common.primitives.Int32BytesSizePrefixed
import io.github.vooft.kafka.serialization.common.primitives.int32ListOf
import io.github.vooft.kafka.serialization.common.primitives.toInt32List
import io.github.vooft.kafka.serialization.common.wrappers.GroupId
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.MemberId
import io.github.vooft.kafka.serialization.common.wrappers.NodeId
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex
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

    @Suppress("detekt:UnusedPrivateProperty")
    private val heartbeatJob = coroutineScope.launch {
        sendHeartbeatInfinite()
    }

    private val consumerMetadata = coroutineScope.async {
        val metadata = rejoinGroup(MemberId.EMPTY)
        MutableStateFlow(metadata)
    }

    private val delegate = SimpleKafkaTopicConsumer(
        topicStateProvider = GroupedTopicMetadataProvider(),
        connectionPool = connectionPool,
        offsetProvider = ConsumerGroupOffsetProvider(
            groupId = groupId,
            topicStateProvider = topicStateProvider,
            groupMembershipProvider = { consumerMetadata.await().value.membership },
            connectionPool = connectionPool
        ),
        coroutineScope = coroutineScope
    )

    override suspend fun consume() = delegate.consume()

    private suspend fun rejoinGroup(memberId: MemberId): ExtendedConsumerMetadata {
        val joinedGroup = joinGroup(memberId)
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
                REBALANCE_IN_PROGRESS, NOT_COORDINATOR, ILLEGAL_GENERATION ->
                    metadata.value = rejoinGroup(metadata.value.membership.memberId)

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
        while (true) { // TODO: better retry
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

    private suspend fun joinGroup(memberId: MemberId = MemberId("")): JoinedGroup {
        while (true) { // TODO: better retry
            val coordinatorNodeId = findCoordinator()

            val connection = connectionPool.acquire(coordinatorNodeId)

            val response = connection.sendRequest<JoinGroupRequestV1, JoinGroupResponseV1>(
                JoinGroupRequestV1(
                    groupId = groupId,
                    sessionTimeoutMs = 30000,
                    rebalanceTimeoutMs = 60000,
                    memberId = memberId,
                    protocolType = CONSUMER_PROTOCOL_TYPE.toInt16String(),
                    groupProtocols = int32ListOf(
                        JoinGroupRequestV1.GroupProtocol(
                            protocol = "mybla".toInt16String(), // TODO: change to proper assigner
                            metadata = Int32BytesSizePrefixed(
                                JoinGroupRequestV1.GroupProtocol.Metadata(
                                    topics = int32ListOf(topic)
                                )
                            )
                        )
                    )
                )
            )

            if (response.errorCode.isRetriable || response.errorCode == NOT_COORDINATOR) {
                continue
            }

            require(response.errorCode == NO_ERROR) { "Join group failed with error code ${response.errorCode}" }

            return JoinedGroup(
                coordinatorNodeId = coordinatorNodeId,
                memberId = response.memberId,
                isLeader = response.leaderId == response.memberId,
                generationId = response.generationId,
                otherMembers = response.members.map { it.memberId }
            )
        }
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
        val syncResponse = connection.sendRequest<SyncGroupRequestV1, SyncGroupResponseV1>(
            SyncGroupRequestV1(
                groupId = groupId,
                generationId = joinedGroup.generationId,
                memberId = joinedGroup.memberId,
                assignments = assignments.map { (memberId, partitions) ->
                    SyncGroupRequestV1.Assignment(
                        memberId = memberId,
                        assignment = Int32BytesSizePrefixed(
                            MemberAssignment(
                                partitionAssignments = int32ListOf(
                                    MemberAssignment.PartitionAssignment(
                                        topic = topic,
                                        partitions = partitions.toInt32List()
                                    )
                                ),
                            )
                        )
                    )
                }.toInt32List()
            )
        )

        return syncResponse.assignment.value?.partitionAssignments?.single()?.partitions?.value ?: listOf()

    }

    inner class GroupedTopicMetadataProvider : KafkaTopicStateProvider {
        override val topic: KafkaTopic get() = this@KafkaGroupedTopicConsumer.topic
        override suspend fun topicPartitions(): Map<PartitionIndex, NodeId> {
            val assignedPartitions = consumerMetadata.await().value.assignedPartitions
            return topicStateProvider.topicPartitions().filterKeys { it in assignedPartitions }
        }
    }
}
