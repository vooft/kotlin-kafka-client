package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.common.PartitionIndex
import io.github.vooft.kafka.serialization.common.IntEncoding.INT32
import io.github.vooft.kafka.serialization.common.KafkaSizeInBytesPrefixed
import io.github.vooft.kafka.serialization.common.customtypes.Int16String
import io.github.vooft.kafka.serialization.common.customtypes.NullableInt16String
import kotlinx.serialization.Serializable

sealed interface ProduceRequest : KafkaRequest {
    override val apiKey: ApiKey get() = ApiKey.PRODUCE
}

/**
 * Produce Request (Version: 3) => transactional_id acks timeout_ms [topic_data]
 *   transactional_id => NULLABLE_STRING
 *   acks => INT16
 *   timeout_ms => INT32
 *   topic_data => name [partition_data]
 *     name => STRING
 *     partition_data => index records
 *       index => INT32
 *       records => RECORDS
 */
@Serializable
data class ProduceRequestV3(
    val transactionalId: NullableInt16String = NullableInt16String.NULL,
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
            val partition: PartitionIndex,
            @KafkaSizeInBytesPrefixed(encoding = INT32) val batchContainer: KafkaRecordBatchContainerV0
        )
    }
}

sealed interface ProduceResponse : KafkaResponse

/**
 * Produce Response (Version: 3) => [responses] throttle_time_ms
 *   responses => name [partition_responses]
 *     name => STRING
 *     partition_responses => index error_code base_offset log_append_time_ms
 *       index => INT32
 *       error_code => INT16
 *       base_offset => INT64
 *       log_append_time_ms => INT64
 *   throttle_time_ms => INT32
 */
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
            val index: PartitionIndex,
            val errorCode: ErrorCode,
            val baseOffset: Long,
            val logAppendTime: Long
        )
    }
}
