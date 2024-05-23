package io.github.vooft.kafka.producer.requests

import io.github.vooft.kafka.network.common.toInt16String
import io.github.vooft.kafka.network.common.toVarInt
import io.github.vooft.kafka.network.common.toVarIntByteArray
import io.github.vooft.kafka.network.messages.KafkaRecordBatchContainerV0
import io.github.vooft.kafka.network.messages.KafkaRecordV0
import io.github.vooft.kafka.network.messages.ProduceRequestV3
import io.github.vooft.kafka.producer.ProducedRecord

object ProduceRequestFactory {
    fun createProduceRequest(topic: String, records: List<ProducedRecord>) = ProduceRequestV3(
        topicData = listOf(
            ProduceRequestV3.TopicData(
                name = topic.toInt16String(),
                partitionData = listOf(
                    ProduceRequestV3.TopicData.PartitionData(
                        partitionIndex = 0,
                        batchContainer = KafkaRecordBatchContainerV0(
                            batch = KafkaRecordBatchContainerV0.KafkaRecordBatch(
                                body = KafkaRecordBatchContainerV0.KafkaRecordBatch.KafkaRecordBatchBody(
                                    lastOffsetDelta = records.size - 1,
                                    firstTimestamp = 0,
                                    maxTimestamp = 0,
                                    records = records.mapIndexed { index, it ->
                                        KafkaRecordV0(
                                            recordBody = KafkaRecordV0.KafkaRecordBody(
                                                offsetDelta = index.toVarInt(),
                                                recordKey = it.key.toVarIntByteArray(),
                                                recordValue = it.value.toVarIntByteArray()
                                            )
                                        )
                                    }

                                )
                            )
                        )

                    )
                )
            )
        )
    )
}
