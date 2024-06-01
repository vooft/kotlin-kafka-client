package io.github.vooft.kafka.consumer.offset

import io.github.vooft.kafka.cluster.KafkaConnectionPool
import io.github.vooft.kafka.cluster.KafkaTopicStateProvider
import io.github.vooft.kafka.consumer.group.ConsumerGroupMembership
import io.github.vooft.kafka.network.common.ErrorCode
import io.github.vooft.kafka.network.messages.OffsetCommitRequestV1
import io.github.vooft.kafka.network.messages.OffsetCommitResponseV1
import io.github.vooft.kafka.network.messages.OffsetFetchRequestV1
import io.github.vooft.kafka.network.messages.OffsetFetchResponseV1
import io.github.vooft.kafka.network.sendRequest
import io.github.vooft.kafka.serialization.common.primitives.int32ListOf
import io.github.vooft.kafka.serialization.common.wrappers.GroupId
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex
import io.github.vooft.kafka.serialization.common.wrappers.PartitionOffset
import kotlinx.datetime.Clock
import org.kodein.log.LoggerFactory
import org.kodein.log.newLogger

class ConsumerGroupOffsetProvider(
    override val groupId: GroupId,
    private val topicStateProvider: KafkaTopicStateProvider,
    private val groupMembershipProvider: ConsumerGroupMembershipProvider,
    private val connectionPool: KafkaConnectionPool
) : ConsumerOffsetProvider {

    override val topic: KafkaTopic get() = topicStateProvider.topic

    // TODO: add caching for offsets

    override suspend fun currentOffset(partition: PartitionIndex): PartitionOffset {
        logger.info { "$groupId: Requesting offset for partition $partition" }

        val membership = groupMembershipProvider.membership()
        val response = connectionPool.acquire(membership.coordinatorNodeId).sendRequest<OffsetFetchRequestV1, OffsetFetchResponseV1>(
            OffsetFetchRequestV1(
                groupId = groupId,
                topics = int32ListOf(
                    OffsetFetchRequestV1.Topic(
                        topic = topic,
                        partitions = int32ListOf(partition)
                    )
                )
            )
        )

        return response.topics.singleOrNull()?.partitions?.singleOrNull()?.offset ?: PartitionOffset(-1)
    }

    override suspend fun commitOffset(partition: PartitionIndex, offset: PartitionOffset) {
        logger.info { "$groupId: Committing offset for partition $partition: $offset" }

        val membership = groupMembershipProvider.membership()
        val response = connectionPool.acquire(membership.coordinatorNodeId).sendRequest<OffsetCommitRequestV1, OffsetCommitResponseV1>(
            OffsetCommitRequestV1(
                groupId = groupId,
                generationIdOrMemberEpoch = membership.generationId,
                memberId = membership.memberId,
                topics = int32ListOf(
                    OffsetCommitRequestV1.Topic(
                        topic = topic,
                        partitions = int32ListOf(
                            OffsetCommitRequestV1.Topic.Partition(
                                partition = partition,
                                committedOffset = offset,
                                commitTimestamp = Clock.System.now().epochSeconds
                            )
                        )
                    )

                )
            )
        )

        val errorCode = response.topics.single().partitions.single().errorCode
        require(errorCode == ErrorCode.NO_ERROR) { "Error committing offset for partition $partition: $errorCode" }
    }

    companion object {
        private val logger = LoggerFactory.default.newLogger<ConsumerGroupOffsetProvider>()
    }
}

fun interface ConsumerGroupMembershipProvider {
    suspend fun membership(): ConsumerGroupMembership
}
