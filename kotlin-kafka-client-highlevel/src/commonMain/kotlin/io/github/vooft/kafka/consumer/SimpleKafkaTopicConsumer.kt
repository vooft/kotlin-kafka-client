package io.github.vooft.kafka.consumer

import io.github.vooft.kafka.cluster.KafkaConnectionPool
import io.github.vooft.kafka.cluster.TopicMetadataProvider
import io.github.vooft.kafka.consumer.requests.ConsumerRequestsFactory
import io.github.vooft.kafka.network.messages.FetchRequestV4
import io.github.vooft.kafka.network.messages.FetchResponseV4
import io.github.vooft.kafka.network.sendRequest
import io.github.vooft.kafka.serialization.common.customtypes.toBuffer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll

class SimpleKafkaTopicConsumer(
    override val topic: String,
    private val topicMetadataProvider: TopicMetadataProvider,
    private val connectionPool: KafkaConnectionPool,
    private val coroutineScope: CoroutineScope = CoroutineScope(Job())
) : KafkaTopicConsumer {

    override suspend fun consume(): KafkaRecordsBatch {
        val topicMetadata = topicMetadataProvider.topicMetadata()

        val partitionsByNode = topicMetadata.partitions.entries.groupBy({ it.value }, { it.key })

        val responses = partitionsByNode.entries.map {
            coroutineScope.async {
                val request = ConsumerRequestsFactory.fetchRequest(topic, it.value)
                connectionPool.acquire(it.key).sendRequest<FetchRequestV4, FetchResponseV4>(request)
            }
        }.awaitAll()

        val records = responses.flatMap { response ->
            response.topics.flatMap { topic ->
                topic.partitions.flatMap { partition ->
                    partition.batchContainer?.let { batchContainer ->
                        batchContainer.batch.body.records.map { record ->
                            KafkaRecord(
                                partition = partition.partition,
                                offset = batchContainer.firstOffset + record.recordBody.offsetDelta.toDecoded(),
                                key = record.recordBody.recordKey.toBuffer(),
                                value = record.recordBody.recordValue.toBuffer()
                            )
                        }
                    } ?: emptyList()
                }
            }
        }

        return KafkaRecordsBatch(topic, records)
    }

}