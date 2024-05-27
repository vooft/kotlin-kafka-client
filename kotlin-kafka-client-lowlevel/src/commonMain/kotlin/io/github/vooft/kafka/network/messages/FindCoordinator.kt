package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.common.NodeId
import io.github.vooft.kafka.serialization.common.ByteValue
import io.github.vooft.kafka.serialization.common.ByteValueSerializer
import io.github.vooft.kafka.serialization.common.customtypes.Int16String
import kotlinx.serialization.Serializable

interface FindCoordinatorRequest : KafkaRequest {
    override val apiKey: ApiKey get() = ApiKey.FIND_COORDINATOR
}

@Serializable
data class FindCoordinatorRequestV1(
    val key: Int16String,
    val keyType: CoordinatorType = CoordinatorType.GROUP
) : FindCoordinatorRequest, VersionedV1

interface FindCoordinatorResponse : KafkaResponse

@Serializable
data class FindCoordinatorResponseV1(
    val throttleTimeMs: Int,
    val errorCode: ErrorCode,
    val errorMessage: Int16String,
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
