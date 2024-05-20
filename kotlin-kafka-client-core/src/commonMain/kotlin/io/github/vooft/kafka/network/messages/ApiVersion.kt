package io.github.vooft.kafka.network.messages

import kotlinx.serialization.Serializable

sealed interface ApiVersionRequest: KafkaRequest {
    override val apiKey: ApiKey get() = ApiKey.API_VERSIONS
}

@Serializable
data object ApiVersionsRequestV1 : ApiVersionRequest, VersionedV1

sealed interface ApiVersionResponse: KafkaResponse

@Serializable
data class ApiVersionsResponseV1(
    val errorCode: Short,
    val apiKeys: List<ApiVersion>,
    val throttleTimeMs: Int = 0
) : ApiVersionResponse, VersionedV1 {
    @Serializable
    data class ApiVersion(
        val apiKey: Short,
        val minVersion: Short,
        val maxVersion: Short
    )
}
