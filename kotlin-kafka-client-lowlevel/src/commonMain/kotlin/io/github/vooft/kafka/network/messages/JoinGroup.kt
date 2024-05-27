package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.common.GroupId
import io.github.vooft.kafka.common.KafkaTopic
import io.github.vooft.kafka.common.MemberId
import io.github.vooft.kafka.serialization.common.IntEncoding.INT32
import io.github.vooft.kafka.serialization.common.KafkaSizeInBytesPrefixed
import io.github.vooft.kafka.serialization.common.customtypes.Int16String
import kotlinx.serialization.Serializable

interface JoinGroupRequest : KafkaRequest {
    override val apiKey: ApiKey get() = ApiKey.JOIN_GROUP
}

/**
 * JoinGroup Request (Version: 1) => group_id session_timeout_ms rebalance_timeout_ms member_id protocol_type [protocols]
 *   group_id => STRING
 *   session_timeout_ms => INT32
 *   rebalance_timeout_ms => INT32
 *   member_id => STRING
 *   protocol_type => STRING
 *   protocols => name metadata
 *     name => STRING
 *     metadata => BYTES
 */
@Serializable
data class JoinGroupRequestV1(
    val groupId: GroupId,
    val sessionTimeoutMs: Int,
    val rebalanceTimeoutMs: Int,
    val memberId: MemberId,
    val protocolType: Int16String,
    val groupProtocols: List<GroupProtocol>
) : JoinGroupRequest, VersionedV1 {
    @Serializable
    data class GroupProtocol(
        val protocol: Int16String,
        @KafkaSizeInBytesPrefixed(INT32) val metadata: Metadata
    ) {
        /**
         * This data structure is not documented in the protocol, structure taken from kafka source code
         */
        @Serializable
        data class Metadata(
            val version: Short = 0,
            val topics: List<KafkaTopic>,
            val userData: List<Byte> = listOf()
        )
    }
}

interface JoinGroupResponse : KafkaResponse

/**
 * JoinGroup Response (Version: 1) => error_code generation_id protocol_name leader member_id [members]
 *   error_code => INT16
 *   generation_id => INT32
 *   protocol_name => STRING
 *   leader => STRING
 *   member_id => STRING
 *   members => member_id metadata
 *     member_id => STRING
 *     metadata => BYTES
 */
@Serializable
data class JoinGroupResponseV1(
    val errorCode: ErrorCode,
    val generationId: Int,
    val groupProtocol: Int16String,
    val leaderId: MemberId,
    val memberId: MemberId,
    val members: List<Member>
) : JoinGroupResponse, VersionedV1 {
    @Serializable
    data class Member(
        val memberId: MemberId,
        val metadata: ByteArray
    )
}
