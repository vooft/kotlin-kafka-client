package io.github.vooft.kafka.network.dtos

import kotlinx.serialization.Serializable

// TODO: add test checking that no raw strings are present
@Serializable
sealed interface KafkaRequest : Versioned {
    val apiKey: ApiKey
}
