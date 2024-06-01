package io.github.vooft.kafka.consumer.offset

import io.github.vooft.kafka.cluster.KafkaConnectionPool
import io.github.vooft.kafka.cluster.KafkaTopicStateProvider
import io.github.vooft.kafka.network.common.ErrorCode
import io.github.vooft.kafka.network.messages.OffsetFetchRequestV1
import io.github.vooft.kafka.network.messages.OffsetFetchResponseV1
import io.github.vooft.kafka.network.sendRequest
import io.github.vooft.kafka.serialization.common.primitives.int32ListOf
import io.github.vooft.kafka.serialization.common.primitives.toInt32List
import io.github.vooft.kafka.serialization.common.wrappers.GroupId
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex
import io.github.vooft.kafka.serialization.common.wrappers.PartitionOffset
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.kodein.log.LoggerFactory
import org.kodein.log.newLogger

class ConsumerGroupOffsetProvider(
    override val groupId: GroupId,
    private val topicStateProvider: KafkaTopicStateProvider,
    private val connectionPool: KafkaConnectionPool
) : ConsumerOffsetProvider {

    override val topic: KafkaTopic get() = topicStateProvider.topic

    private val offsets = mutableMapOf<PartitionIndex, PartitionOffset>()
    private val offsetsMutex = Mutex()

    private val mutex = Mutex()

    override suspend fun currentOffset(partition: PartitionIndex): PartitionOffset = mutex.withLock {
        val existing = offsetsMutex.withLock { offsets[partition] }
        if (existing != null) {
            logger.info { "$groupId: Found existing offset for partition $partition: $existing" }
            return existing
        }

        val topicPartitions = topicStateProvider.topicPartitions()
        logger.info { "$groupId: Requesting offsets for partitions ${topicPartitions.keys}" }

        val response = connectionPool.acquire().sendRequest<OffsetFetchRequestV1, OffsetFetchResponseV1>(
            OffsetFetchRequestV1(
                groupId = groupId,
                topics = int32ListOf(
                    OffsetFetchRequestV1.Topic(
                        topic = topic,
                        partitions = topicPartitions.keys.toInt32List()
                    )
                )
            )
        )

        response.topics.singleOrNull()?.let { topic ->
            offsetsMutex.withLock {
                topic.partitions.forEach {
                    require(it.errorCode == ErrorCode.NO_ERROR) { "Error fetching offset: ${it.errorCode}" }
                    logger.info { "$groupId: Recording offset for partition ${it.partition}: ${it.offset}" }
                    offsets[it.partition] = it.offset
                }
            }
        }

        return offsetsMutex.withLock { offsets[partition] } ?: PartitionOffset(-1)
    }

    override suspend fun commitOffset(partition: PartitionIndex, offset: PartitionOffset) {
        logger.info { "$groupId: Recording offset for partition $partition: $offset" }
        offsetsMutex.withLock {
            offsets[partition] = offset
        }
    }

    companion object {
        private val logger = LoggerFactory.default.newLogger<ConsumerGroupOffsetProvider>()
    }
}
