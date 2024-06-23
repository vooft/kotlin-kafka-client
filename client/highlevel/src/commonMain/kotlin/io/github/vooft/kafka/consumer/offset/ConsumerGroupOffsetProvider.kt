package io.github.vooft.kafka.consumer.offset

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.vooft.kafka.cluster.KafkaConnectionPool
import io.github.vooft.kafka.cluster.KafkaTopicStateProvider
import io.github.vooft.kafka.consumer.group.ConsumerGroupMembership
import io.github.vooft.kafka.network.commitOffset
import io.github.vooft.kafka.network.common.ErrorCode
import io.github.vooft.kafka.network.fetchOffset
import io.github.vooft.kafka.serialization.common.wrappers.GroupId
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex
import io.github.vooft.kafka.serialization.common.wrappers.PartitionOffset

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
        val response = connectionPool.acquire(membership.coordinatorNodeId).fetchOffset(groupId, topic, listOf(partition))

        return response.topics.singleOrNull()?.partitions?.singleOrNull()?.offset ?: PartitionOffset(-1)
    }

    override suspend fun commitOffset(partition: PartitionIndex, offset: PartitionOffset) {
        logger.info { "$groupId: Committing offset for partition $partition: $offset" }

        val membership = groupMembershipProvider.membership()
        val response = connectionPool.acquire(membership.coordinatorNodeId).commitOffset(
            groupId = groupId,
            generationId = membership.generationId,
            memberId = membership.memberId,
            topic = topic,
            offsets = mapOf(partition to offset)
        )

        val errorCode = response.topics.single().partitions.single().errorCode
        require(errorCode == ErrorCode.NO_ERROR) { "Error committing offset for partition $partition: $errorCode" }
    }

    companion object {
        private val logger = KotlinLogging.logger {  }
    }
}

fun interface ConsumerGroupMembershipProvider {
    suspend fun membership(): ConsumerGroupMembership
}
