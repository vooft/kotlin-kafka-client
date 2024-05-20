package io.github.vooft.kafka.network.messages

import kotlinx.serialization.Serializable

@Serializable
data object ApiVersionsRequestV0 : KafkaRequest, VersionedV0 {
    override val apiKey: ApiKey = ApiKey.API_VERSIONS
}

@Serializable
data class ApiVersionsResponseV0(
    val errorCode: Short,
    val apiKeys: List<ApiVersion>,
) : KafkaResponse, VersionedV0 {
    @Serializable
    data class ApiVersion(
        val apiKey: Short,
        val minVersion: Short,
        val maxVersion: Short
    )
}
