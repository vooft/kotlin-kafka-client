package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.network.common.ErrorCode
import io.github.vooft.kafka.serialization.common.primitives.Int32BytesSizePrefixed
import io.github.vooft.kafka.serialization.common.primitives.Int32List
import io.github.vooft.kafka.serialization.common.primitives.int32ListOf
import io.github.vooft.kafka.serialization.common.wrappers.GroupId
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.MemberId
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex
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
    val groupId: GroupId,
    val generationId: Int,
    val memberId: MemberId,
    val assignments: Int32List<Assignment>
) : SyncGroupRequest, VersionedV1 {
    @Serializable
    data class Assignment(
        val memberId: MemberId,
        val assignment: Int32BytesSizePrefixed<MemberAssignment>
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
    val assignment: Int32BytesSizePrefixed<MemberAssignment?>
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
    val partitionAssignments: Int32List<PartitionAssignment>,
    val userData: Int32List<Byte> = int32ListOf()
) {
    @Serializable
    data class PartitionAssignment(
        val topic: KafkaTopic,
        val partitions: Int32List<PartitionIndex>
    )
}
