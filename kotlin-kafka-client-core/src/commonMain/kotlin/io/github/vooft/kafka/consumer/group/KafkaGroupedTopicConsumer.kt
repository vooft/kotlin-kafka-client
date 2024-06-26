package io.github.vooft.kafka.consumer.group

import io.github.vooft.kafka.cluster.KafkaConnectionPool
import io.github.vooft.kafka.cluster.KafkaTopicStateProvider
import io.github.vooft.kafka.common.types.GroupId
import io.github.vooft.kafka.common.types.KafkaTopic
import io.github.vooft.kafka.common.types.MemberId
import io.github.vooft.kafka.common.types.NodeId
import io.github.vooft.kafka.common.types.PartitionIndex
import io.github.vooft.kafka.consumer.KafkaTopicConsumer
import io.github.vooft.kafka.consumer.SimpleKafkaTopicConsumer
import io.github.vooft.kafka.consumer.offset.ConsumerGroupOffsetProvider
import io.github.vooft.kafka.transport.common.ErrorCode
import io.github.vooft.kafka.transport.common.ErrorCode.ILLEGAL_GENERATION
import io.github.vooft.kafka.transport.common.ErrorCode.NOT_COORDINATOR
import io.github.vooft.kafka.transport.common.ErrorCode.NO_ERROR
import io.github.vooft.kafka.transport.common.ErrorCode.REBALANCE_IN_PROGRESS
import io.github.vooft.kafka.transport.common.ErrorCode.UNKNOWN_MEMBER_ID
import io.github.vooft.kafka.transport.findGroupCoordinator
import io.github.vooft.kafka.transport.heartbeat
import io.github.vooft.kafka.transport.joinGroup
import io.github.vooft.kafka.transport.syncGroup
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
        val response = connection.heartbeat(
            groupId = groupId,
            generationId = metadata.membership.generationId,
            memberId = metadata.membership.memberId
        )

        return response.errorCode
    }

    private suspend fun findCoordinator(): NodeId {
        while (true) { // TODO: better retry
            val connection = connectionPool.acquire()
            val response = connection.findGroupCoordinator(groupId)

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

            val response = connection.joinGroup(
                groupId = groupId,
                memberId = memberId,
                topic = topic
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
        val syncResponse = connection.syncGroup(
            groupId = groupId,
            topic = topic,
            generationId = joinedGroup.generationId,
            currentMemberId = joinedGroup.memberId,
            assignments = assignments
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
