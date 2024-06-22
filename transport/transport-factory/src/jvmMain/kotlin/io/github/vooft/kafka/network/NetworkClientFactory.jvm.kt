package io.github.vooft.kafka.network

import io.github.vooft.kafka.network.ktor.KtorNetworkClient

actual fun NetworkClient.Companion.createDefaultClient(): NetworkClient {
    return KtorNetworkClient()
}
