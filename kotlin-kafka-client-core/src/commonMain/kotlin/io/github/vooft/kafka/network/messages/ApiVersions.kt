package io.github.vooft.kafka.network.messages

import kotlinx.serialization.Serializable

sealed interface ApiVersionRequest: KafkaRequest

@Serializable
data object ApiVersionsRequestV1 : ApiVersionRequest, VersionedV1 {
    override val apiKey: ApiKey = ApiKey.API_VERSIONS
}

@Serializable
data class ApiVersionsResponseV1(
    val errorCode: Short,
    val apiKeys: List<ApiVersion>,
    val throttleTimeMs: Int = 0
) : KafkaResponse, VersionedV1 {
    @Serializable
    data class ApiVersion(
        val apiKey: Short,
        val minVersion: Short,
        val maxVersion: Short
    )
}
