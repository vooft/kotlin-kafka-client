package io.github.vooft.kafka.network.messages

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
            @KafkaSizeInBytesPrefixed val batchContainer: KafkaRecordBatchContainer
        )
    }
}

sealed interface ProduceResponse : KafkaResponse

@Serializable
data class ProduceResponseV1(
    val topicResponses: List<TopicResponse>,
    val throttleTimeMs: Int
) : ProduceResponse, VersionedV1 {
    @Serializable
    data class TopicResponse(
        val name: Int16String,
        val partitionResponses: List<PartitionResponse>
    ) {
        @Serializable
        data class PartitionResponse(
            val index: Int,
            val errorCode: ErrorCode,
            val baseOffset: Long,
        )
    }
}

// [0x00, 0x00, 0x02, 0x9A, // corr id
// 0x00, 0x00, 0x00, 0x01,
// 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00]
