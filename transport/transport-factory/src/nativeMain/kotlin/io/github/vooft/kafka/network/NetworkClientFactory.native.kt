package io.github.vooft.kafka.network

import io.github.vooft.kafka.network.ktor.KtorKafkaTransport

actual fun KafkaTransport.Companion.createDefaultClient(): KafkaTransport {
    return KtorKafkaTransport()
}
