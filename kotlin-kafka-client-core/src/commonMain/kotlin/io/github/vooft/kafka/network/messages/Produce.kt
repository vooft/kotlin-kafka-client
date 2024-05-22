package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.serialization.common.IntEncoding.INT32
import io.github.vooft.kafka.serialization.common.KafkaSizeInBytesPrefixed
import io.github.vooft.kafka.serialization.common.customtypes.Int16String
import kotlinx.serialization.Serializable

sealed interface ProduceRequest : KafkaRequest {
    override val apiKey: ApiKey get() = ApiKey.PRODUCE
}

@Serializable
data class ProduceRequestV3(
    val transactionalId: Int16String = Int16String.NULL,
    val acks: Short = -1,
    val timeoutMs: Int = 1000,
    val topicData: List<TopicData>
) : ProduceRequest, VersionedV3 {
    @Serializable
    data class TopicData(
        val name: Int16String,
        val partitionData: List<PartitionData>
    ) {
        @Serializable
        data class PartitionData(
            val partitionIndex: Int,
            @KafkaSizeInBytesPrefixed(encoding = INT32) val batchContainer: KafkaRecordBatchContainer
        )
    }
}

sealed interface ProduceResponse : KafkaResponse

@Serializable
data class ProduceResponseV3(
    val topicResponses: List<TopicResponse>,
    val throttleTimeMs: Int
) : ProduceResponse, VersionedV3 {
    @Serializable
    data class TopicResponse(
        val topicName: Int16String,
        val partitionResponses: List<PartitionResponse>
    ) {
        @Serializable
        data class PartitionResponse(
            val index: Int,
            val errorCode: ErrorCode,
            val baseOffset: Long,
            val logAppendTime: Long
        )
    }
}
