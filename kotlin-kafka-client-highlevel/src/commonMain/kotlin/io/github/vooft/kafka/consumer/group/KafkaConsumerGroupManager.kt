package io.github.vooft.kafka.consumer.group

import io.github.vooft.kafka.cluster.KafkaConnectionPoolFactory
import io.github.vooft.kafka.cluster.KafkaMetadataManager
import io.github.vooft.kafka.common.GroupId
import io.github.vooft.kafka.common.KafkaTopic
import io.github.vooft.kafka.common.MemberId
import io.github.vooft.kafka.common.NodeId
import io.github.vooft.kafka.common.PartitionIndex
import io.github.vooft.kafka.consumer.KafkaTopicConsumer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart.LAZY
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.jvm.JvmInline

class KafkaConsumerGroupManager(
    private val topic: KafkaTopic,
    private val groupId: GroupId,
    private val metadataManager: KafkaMetadataManager,
    private val connectionPoolFactory: KafkaConnectionPoolFactory,
    private val coroutineScope: CoroutineScope = CoroutineScope(Job())
) {

    private val topicMetadataProviderDeferred = coroutineScope.async(start = LAZY) {
        metadataManager.topicMetadataProvider(topic)
    }

    private val consumers = mutableMapOf<InternalConsumerId, KafkaGroupedTopicConsumer>()
    private val consumersMutex = Mutex()

    suspend fun createConsumer(): KafkaTopicConsumer {
        val consumerId = InternalConsumerId.next()
        val consumer = KafkaGroupedTopicConsumer(
            topic = topic,
            groupId = groupId,
            topicMetadataProvider = topicMetadataProviderDeferred.await(),
            connectionPool = connectionPoolFactory.create(),
            coroutineScope = coroutineScope
        )

        consumersMutex.withLock {
            consumers[consumerId] = consumer
        }

        return consumer
    }
}

data class ExtendedConsumerMetadata(
    override val membership: JoinedGroup,
    override val assignedPartitions: List<PartitionIndex>,
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
const val CONSUMER_PROTOCOL_TYPE = "consumer"
