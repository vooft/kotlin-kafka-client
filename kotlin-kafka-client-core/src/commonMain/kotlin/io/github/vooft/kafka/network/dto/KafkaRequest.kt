package io.github.vooft.kafka.network.dto

sealed interface KafkaRequest {
    val apiKey: ApiKey
}

data object ApiVersionRequest : KafkaRequest {
    override val apiKey: ApiKey = ApiKey.API_VERSIONS
}
