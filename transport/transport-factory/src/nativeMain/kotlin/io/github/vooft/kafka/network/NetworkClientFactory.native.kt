package io.github.vooft.kafka.network

import io.github.vooft.kafka.network.ktor.KtorKafkaTransport
import kotlinx.coroutines.CoroutineScope

actual fun KafkaTransport.Companion.createDefaultClient(coroutineScope: CoroutineScope): KafkaTransport {
    return KtorKafkaTransport()
}
