package io.github.vooft.kafka.network.messages

import kotlinx.serialization.Serializable

@Serializable
data object ApiVersionsRequestV0 : KafkaRequest, VersionV0 {
    override val apiKey: Short = ApiKey.API_VERSIONS
}

@Serializable
data class ApiVersionsResponseV0(
    val errorCode: Short,
    val apiKeys: List<ApiVersion>,
) : KafkaResponse, VersionV0 {
    @Serializable
    data class ApiVersion(
        val apiKey: Short,
        val minVersion: Short,
        val maxVersion: Short
    )
}
