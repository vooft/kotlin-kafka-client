package io.github.vooft.kafka.transport.dtos

// TODO: add test checking that no raw strings are present
interface KafkaRequest : Versioned {
    val apiKey: ApiKey
}
