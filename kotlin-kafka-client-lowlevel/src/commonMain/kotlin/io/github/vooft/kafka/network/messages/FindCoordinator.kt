package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.common.NodeId
import io.github.vooft.kafka.network.common.ErrorCode
import io.github.vooft.kafka.serialization.common.ByteValue
import io.github.vooft.kafka.serialization.common.ByteValueSerializer
import io.github.vooft.kafka.serialization.common.primitives.Int16String
import io.github.vooft.kafka.serialization.common.primitives.NullableInt16String
import kotlinx.serialization.Serializable

interface FindCoordinatorRequest : KafkaRequest {
    override val apiKey: ApiKey get() = ApiKey.FIND_COORDINATOR
}

/**
 * FindCoordinator Request (Version: 1) => key key_type
 *   key => STRING
 *   key_type => INT8
 */
@Serializable
data class FindCoordinatorRequestV1(
    val key: Int16String,
    val keyType: CoordinatorType = CoordinatorType.GROUP
) : FindCoordinatorRequest, VersionedV1

interface FindCoordinatorResponse : KafkaResponse

/**
 * FindCoordinator Response (Version: 1) => throttle_time_ms error_code error_message node_id host port
 *   throttle_time_ms => INT32
 *   error_code => INT16
 *   error_message => NULLABLE_STRING
 *   node_id => INT32
 *   host => STRING
 *   port => INT32
 */
@Serializable
data class FindCoordinatorResponseV1(
    val throttleTimeMs: Int,
    val errorCode: ErrorCode,
    val errorMessage: NullableInt16String,
    val nodeId: NodeId,
    val host: Int16String,
    val port: Int,
) : FindCoordinatorResponse, VersionedV1

@Serializable(with = CoordinatorTypeSerializer::class)
enum class CoordinatorType(override val value: Byte) : ByteValue {
    GROUP(0),
    TRANSACTION(1),
}

object CoordinatorTypeSerializer : ByteValueSerializer<CoordinatorType>(CoordinatorType.entries)
