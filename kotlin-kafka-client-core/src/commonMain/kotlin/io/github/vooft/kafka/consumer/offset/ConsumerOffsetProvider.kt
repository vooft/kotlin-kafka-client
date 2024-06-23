package io.github.vooft.kafka.consumer.offset

import io.github.vooft.kafka.common.types.GroupId
import io.github.vooft.kafka.common.types.KafkaTopic
import io.github.vooft.kafka.common.types.PartitionIndex
import io.github.vooft.kafka.common.types.PartitionOffset
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

interface ConsumerOffsetProvider {
    val topic: KafkaTopic
    val groupId: GroupId?

    suspend fun currentOffset(partition: PartitionIndex): PartitionOffset
    suspend fun commitOffset(partition: PartitionIndex, offset: PartitionOffset)
}

class InMemoryConsumerOffsetProvider(
    override val topic: KafkaTopic,
    initialOffsets: Map<PartitionIndex, PartitionOffset> = emptyMap()
) : ConsumerOffsetProvider {

    override val groupId: GroupId? = null

    private val offsets = initialOffsets.toMutableMap()
    private val offsetsMutex = Mutex()

    override suspend fun currentOffset(partition: PartitionIndex): PartitionOffset {
        return offsetsMutex.withLock { offsets[partition] } ?: PartitionOffset(-1)
    }

    override suspend fun commitOffset(partition: PartitionIndex, offset: PartitionOffset) {
        offsetsMutex.withLock { offsets[partition] = offset }
    }
}

