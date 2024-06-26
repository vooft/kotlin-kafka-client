package io.github.vooft.kafka.consumer

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.vooft.kafka.cluster.KafkaConnectionPool
import io.github.vooft.kafka.cluster.KafkaTopicStateProvider
import io.github.vooft.kafka.common.types.KafkaTopic
import io.github.vooft.kafka.consumer.offset.ConsumerOffsetProvider
import io.github.vooft.kafka.consumer.offset.InMemoryConsumerOffsetProvider
import io.github.vooft.kafka.serialization.common.primitives.toBuffer
import io.github.vooft.kafka.transport.fetch
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll

class SimpleKafkaTopicConsumer(
    private val topicStateProvider: KafkaTopicStateProvider,
    private val connectionPool: KafkaConnectionPool,
    private val offsetProvider: ConsumerOffsetProvider = InMemoryConsumerOffsetProvider(topicStateProvider.topic),
    private val autoCommitOffset: Boolean = true,
    private val coroutineScope: CoroutineScope
) : KafkaTopicConsumer {

    init {
        require(autoCommitOffset) { "Only auto commit offset is supported" }
    }

    override val topic: KafkaTopic get() = topicStateProvider.topic

    override suspend fun consume(): KafkaRecordsBatch {
        val partitionsByNode = topicStateProvider.topicPartitions().entries.groupBy(
            keySelector = { it.value },
            valueTransform = { it.key }
        )

        logger.info { "Consuming topic $topic from partitions: ${partitionsByNode.values.flatten()}" }

        val responses = partitionsByNode.entries.map {
            coroutineScope.async {
                val partitionOffsets = it.value.associateWith { offsetProvider.currentOffset(it) + 1 }
                connectionPool.acquire(it.key).fetch(topic, partitionOffsets)
            }
        }.awaitAll()

        val records = responses.flatMap { response ->
            response.topics.flatMap { topic ->
                topic.partitions.flatMap { partition ->
                    partition.batchContainer.value?.let { batchContainer ->
                        batchContainer.batch.value.body.value.records.map { record ->
                            KafkaRecord(
                                partition = partition.partition,
                                offset = batchContainer.firstOffset + record.recordBody.value.offsetDelta.toDecoded(),
                                key = record.recordBody.value.recordKey.toBuffer(),
                                value = record.recordBody.value.recordValue.toBuffer()
                            )
                        }
                    } ?: emptyList()
                }
            }
        }

        if (autoCommitOffset) {
            records.groupBy { it.partition }.forEach { (partition, records) ->
                val maxOffset = records.maxOf { it.offset }
                offsetProvider.commitOffset(partition, maxOffset)
            }
        }

        return KafkaRecordsBatch(topic, records)
    }

    companion object {
        private val logger = KotlinLogging.logger { }
    }
}
