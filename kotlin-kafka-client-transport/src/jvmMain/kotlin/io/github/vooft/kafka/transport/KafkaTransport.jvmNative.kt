package io.github.vooft.kafka.transport

import io.github.vooft.kafka.transport.ktor.KtorKafkaTransport
import kotlinx.coroutines.CoroutineScope

actual fun KafkaTransport.Companion.createDefault(coroutineScope: CoroutineScope): KafkaTransport {
    return KtorKafkaTransport()
}
