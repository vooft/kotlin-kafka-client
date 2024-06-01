package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.network.common.ErrorCode
import io.github.vooft.kafka.serialization.common.primitives.Int32List
import io.github.vooft.kafka.serialization.common.primitives.NullableInt16String
import io.github.vooft.kafka.serialization.common.wrappers.GroupId
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.MemberId
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex
import io.github.vooft.kafka.serialization.common.wrappers.PartitionOffset
import kotlinx.serialization.Serializable

interface OffsetCommitRequest : KafkaRequest {
    override val apiKey: ApiKey get() = ApiKey.OFFSET_COMMIT
}

/**
 * OffsetCommit Request (Version: 1) => group_id generation_id_or_member_epoch member_id [topics]
 *   group_id => STRING
 *   generation_id_or_member_epoch => INT32
 *   member_id => STRING
 *   topics => name [partitions]
 *     name => STRING
 *     partitions => partition_index committed_offset commit_timestamp committed_metadata
 *       partition_index => INT32
 *       committed_offset => INT64
 *       commit_timestamp => INT64
 *       committed_metadata => NULLABLE_STRING
 */
@Serializable
data class OffsetCommitRequestV1(
    val groupId: GroupId,
    val generationIdOrMemberEpoch: Int,
    val memberId: MemberId,
    val topics: Int32List<Topic>
): OffsetCommitRequest, VersionedV1 {
    @Serializable
    data class Topic(
        val topic: KafkaTopic,
        val partitions: Int32List<Partition>
    ) {
        @Serializable
        data class Partition(
            val partitionIndex: PartitionIndex,
            val committedOffset: PartitionOffset,
            val commitTimestamp: Long,
            val committedMetadata: NullableInt16String
        )
    }
}

interface OffsetCommitResponse : KafkaResponse

/**
 * OffsetCommit Response (Version: 1) => [topics]
 *   topics => name [partitions]
 *     name => STRING
 *     partitions => partition_index error_code
 *       partition_index => INT32
 *       error_code => INT16
 */
@Serializable
data class OffsetCommitResponseV1(
    val topics: Int32List<Topic>
): OffsetCommitResponse, VersionedV1 {
    @Serializable
    data class Topic(
        val topic: KafkaTopic,
        val partitions: Int32List<Partition>
    ) {
        @Serializable
        data class Partition(
            val partitionIndex: PartitionIndex,
            val errorCode: ErrorCode
        )
    }
}
