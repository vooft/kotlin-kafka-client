package io.github.vooft.kafka.transport.messages

import io.github.vooft.kafka.serialization.common.primitives.Int32List
import io.github.vooft.kafka.transport.common.ErrorCode
import io.github.vooft.kafka.transport.dtos.ApiKey
import io.github.vooft.kafka.transport.dtos.KafkaRequest
import io.github.vooft.kafka.transport.dtos.KafkaResponse
import kotlinx.serialization.Serializable

sealed interface ApiVersionRequest: KafkaRequest {
    override val apiKey: ApiKey get() = ApiKey.API_VERSIONS
}

@Serializable
data object ApiVersionsRequestV1 : ApiVersionRequest, VersionedV1

sealed interface ApiVersionResponse: KafkaResponse

@Serializable
data class ApiVersionsResponseV1(
    val errorCode: ErrorCode,
    val apiKeys: Int32List<ApiVersion>,
    val throttleTimeMs: Int = 0
) : ApiVersionResponse, VersionedV1 {
    @Serializable
    data class ApiVersion(
        val apiKey: Short,
        val minVersion: Short,
        val maxVersion: Short
    )
}
