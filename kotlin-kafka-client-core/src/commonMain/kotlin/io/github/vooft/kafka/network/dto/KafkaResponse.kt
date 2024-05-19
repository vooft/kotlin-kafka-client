package io.github.vooft.kafka.network.dto

import kotlinx.serialization.Serializable

interface KafkaResponse

@Serializable
data class ApiVersionsResponse(
    val errorCode: Short,
    val apiKeys: List<ApiVersion>,
    val throttleTimeMs: Int
) : KafkaResponse {
    @Serializable
    data class ApiVersion(
        val apiKey: Short,
        val minVersion: Short,
        val maxVersion: Short
    )
}
