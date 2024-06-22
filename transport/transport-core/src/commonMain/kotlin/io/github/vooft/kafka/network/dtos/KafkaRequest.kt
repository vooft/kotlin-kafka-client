package io.github.vooft.kafka.network.dtos

// TODO: add test checking that no raw strings are present
interface KafkaRequest : Versioned {
    val apiKey: ApiKey
}
