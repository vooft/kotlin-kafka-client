package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.common.KafkaTopic
import io.github.vooft.kafka.common.PartitionIndex
import io.github.vooft.kafka.serialization.common.customtypes.Int16String
import io.github.vooft.kafka.serialization.common.customtypes.NullableInt16String
import kotlinx.serialization.Serializable

interface OffsetFetchRequest : KafkaRequest {
    override val apiKey: ApiKey get() = ApiKey.OFFSET_FETCH
}

/**
 * OffsetFetch Request (Version: 1) => group_id [topics]
 *   group_id => STRING
 *   topics => name [partition_indexes]
 *     name => STRING
 *     partition_indexes => INT32
 */
@Serializable
data class OffsetFetchRequestV1(
    val groupId: NullableInt16String,
    val topics: List<Topic>
) : OffsetFetchRequest, VersionedV1 {
    @Serializable
    data class Topic(
        val topic: Int16String,
        val partitions: List<Partition>
    ) {
        @Serializable
        data class Partition(
            val partition: PartitionIndex
        )
    }
}

interface OffsetFetchResponse : KafkaResponse

/**
 * OffsetFetch Response (Version: 1) => [topics]
 *   topics => name [partitions]
 *     name => STRING
 *     partitions => partition_index committed_offset metadata error_code
 *       partition_index => INT32
 *       committed_offset => INT64
 *       metadata => NULLABLE_STRING
 *       error_code => INT16
 */
@Serializable
data class OffsetFetchResponseV1(
    val topics: List<Topic>
) : OffsetFetchResponse, VersionedV1 {
    @Serializable
    data class Topic(
        val topic: KafkaTopic,
        val partitions: List<Partition>
    ) {
        @Serializable
        data class Partition(
            val partition: PartitionIndex,
            val offset: Long,
            val metadata: NullableInt16String,
            val errorCode: ErrorCode
        )
    }
}
