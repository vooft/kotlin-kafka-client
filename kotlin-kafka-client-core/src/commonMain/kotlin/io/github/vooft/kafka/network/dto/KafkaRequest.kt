package io.github.vooft.kafka.network.dto

import kotlinx.serialization.Serializable

@Serializable
sealed interface KafkaRequest {
    val apiKey: Short
}

@Serializable
data object ApiVersionRequest : KafkaRequest {
    override val apiKey: Short = ApiKey.API_VERSIONS
}
