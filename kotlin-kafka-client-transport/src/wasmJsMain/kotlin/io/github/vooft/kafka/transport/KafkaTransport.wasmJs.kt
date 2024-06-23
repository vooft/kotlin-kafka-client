package io.github.vooft.kafka.transport

import io.github.vooft.kafka.transport.nodejs.NodeJsKafkaTransport
import kotlinx.coroutines.CoroutineScope

actual fun KafkaTransport.Companion.createDefault(coroutineScope: CoroutineScope): KafkaTransport {
    return NodeJsKafkaTransport(coroutineScope)
}
