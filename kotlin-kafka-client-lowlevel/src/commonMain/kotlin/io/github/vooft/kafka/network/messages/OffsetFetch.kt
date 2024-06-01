package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.network.common.ErrorCode
import io.github.vooft.kafka.serialization.common.primitives.Int16String
import io.github.vooft.kafka.serialization.common.primitives.Int32List
import io.github.vooft.kafka.serialization.common.primitives.NullableInt16String
import io.github.vooft.kafka.serialization.common.wrappers.GroupId
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex
import io.github.vooft.kafka.serialization.common.wrappers.PartitionOffset
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
        val topic: Int16String,
        val partitions: Int32List<Partition>
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
