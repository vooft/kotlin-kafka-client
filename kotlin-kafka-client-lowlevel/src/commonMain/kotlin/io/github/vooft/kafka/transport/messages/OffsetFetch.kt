package io.github.vooft.kafka.transport.messages

import io.github.vooft.kafka.common.types.GroupId
import io.github.vooft.kafka.common.types.KafkaTopic
import io.github.vooft.kafka.common.types.PartitionIndex
import io.github.vooft.kafka.common.types.PartitionOffset
import io.github.vooft.kafka.serialization.common.primitives.Int32List
import io.github.vooft.kafka.serialization.common.primitives.NullableInt16String
import io.github.vooft.kafka.transport.common.ErrorCode
import io.github.vooft.kafka.transport.dtos.ApiKey
import io.github.vooft.kafka.transport.dtos.KafkaRequest
import io.github.vooft.kafka.transport.dtos.KafkaResponse
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
    val groupId: GroupId,
    val topics: Int32List<Topic>
) : OffsetFetchRequest, VersionedV1 {
    @Serializable
    data class Topic(
        val topic: KafkaTopic,
        val partitions: Int32List<PartitionIndex>
    )
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
    val topics: Int32List<Topic>
) : OffsetFetchResponse, VersionedV1 {
    @Serializable
    data class Topic(
        val topic: KafkaTopic,
        val partitions: Int32List<Partition>
    ) {
        @Serializable
        data class Partition(
            val partition: PartitionIndex,
            val offset: PartitionOffset,
            val metadata: NullableInt16String,
            val errorCode: ErrorCode
        )
    }
}
