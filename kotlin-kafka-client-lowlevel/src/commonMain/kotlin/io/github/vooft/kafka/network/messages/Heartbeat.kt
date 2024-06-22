package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.network.common.ErrorCode
import io.github.vooft.kafka.network.dtos.ApiKey
import io.github.vooft.kafka.network.dtos.KafkaRequest
import io.github.vooft.kafka.network.dtos.KafkaResponse
import io.github.vooft.kafka.serialization.common.wrappers.GroupId
import io.github.vooft.kafka.serialization.common.wrappers.MemberId
import kotlinx.serialization.Serializable

interface HeartbeatRequest : KafkaRequest {
    override val apiKey: ApiKey get() = ApiKey.HEARTBEAT
}

/**
 * Heartbeat Request (Version: 0) => group_id generation_id member_id
 *   group_id => STRING
 *   generation_id => INT32
 *   member_id => STRING
 */
@Serializable
data class HeartbeatRequestV0(
    val groupId: GroupId,
    val generationId: Int,
    val memberId: MemberId
) : HeartbeatRequest, VersionedV0

interface HeartbeatResponse : KafkaResponse

/**
 * Heartbeat Response (Version: 0) => error_code
 *   error_code => INT16
 */
@Serializable
data class HeartbeatResponseV0(
    val errorCode: ErrorCode
) : HeartbeatResponse, VersionedV0
