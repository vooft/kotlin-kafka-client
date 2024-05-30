package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.network.common.ErrorCode
import io.github.vooft.kafka.serialization.common.primitives.Int32BytesSizePrefixed
import io.github.vooft.kafka.serialization.common.primitives.Int32List
import io.github.vooft.kafka.serialization.common.primitives.NullableInt16String
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex
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
    val timeoutMs: Int,
    val topic: Int32List<Topic>
) : ProduceRequest, VersionedV3 {
    @Serializable
    data class Topic(
        val topic: KafkaTopic,
        val partition: Int32List<Partition>
    ) {
        @Serializable
        data class Partition(
            val partition: PartitionIndex,
            val batchContainer: Int32BytesSizePrefixed<KafkaRecordBatchContainerV0>
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
    val topics: Int32List<Topic>,
    val throttleTimeMs: Int
) : ProduceResponse, VersionedV3 {
    @Serializable
    data class Topic(
        val topic: KafkaTopic,
        val partitions: Int32List<Partition>
    ) {
        @Serializable
        data class Partition(
            val index: PartitionIndex,
            val errorCode: ErrorCode,
            val baseOffset: Long,
            val logAppendTimeMs: Long
        )
    }
}
