package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.common.KafkaTopic
import io.github.vooft.kafka.common.MemberId
import io.github.vooft.kafka.common.PartitionIndex
import io.github.vooft.kafka.serialization.common.IntEncoding.INT32
import io.github.vooft.kafka.serialization.common.KafkaSizeInBytesPrefixed
import io.github.vooft.kafka.serialization.common.customtypes.Int16String
import kotlinx.serialization.Serializable

interface SyncGroupRequest : KafkaRequest {
    override val apiKey: ApiKey get() = ApiKey.SYNC_GROUP
}

/**
 * SyncGroup Request (Version: 1) => group_id generation_id member_id [assignments]
 *   group_id => STRING
 *   generation_id => INT32
 *   member_id => STRING
 *   assignments => member_id assignment
 *     member_id => STRING
 *     assignment => BYTES
 */
@Serializable
data class SyncGroupRequestV1(
    val groupId: Int16String,
    val generationId: Int,
    val memberId: MemberId,
    val assignments: List<Assignment>
) : SyncGroupRequest, VersionedV1 {
    @Serializable
    data class Assignment(
        val memberId: MemberId,
        @KafkaSizeInBytesPrefixed(encoding = INT32) val assignment: MemberAssignment
    )
}

interface SyncGroupResponse : KafkaResponse

/**
 * SyncGroup Response (Version: 1) => throttle_time_ms error_code assignment
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   assignment => BYTES
 */
@Serializable
data class SyncGroupResponseV1(
    val throttleTimeMs: Int,
    val errorCode: ErrorCode,
    // TODO: when there is an error, assignment is not returned at all, even length
    @KafkaSizeInBytesPrefixed(encoding = INT32) val assignment: MemberAssignment?
) : SyncGroupResponse, VersionedV1

/**
 * Custom data structure describing partition assignments??
 *
 * Schema from https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-SyncGroupRequest
 * MemberAssignment => Version PartitionAssignment
 *   Version => int16
 *   PartitionAssignment => [Topic [Partition]]
 *     Topic => string
 *     Partition => int32
 *   UserData => bytes
 */
@Serializable
data class MemberAssignment(
    val version: Short = 0,
    val partitionAssignments: List<PartitionAssignment>,
    val userData: List<Byte> = listOf()
) {
    @Serializable
    data class PartitionAssignment(
        val topic: KafkaTopic,
        val partitions: List<PartitionIndex>
    )
}
