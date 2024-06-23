package io.github.vooft.kafka.network

import io.github.vooft.kafka.network.nodejs.NodeJsKafkaTransport
import kotlinx.coroutines.CoroutineScope

actual fun KafkaTransport.Companion.createDefaultClient(coroutineScope: CoroutineScope): KafkaTransport {
    return NodeJsKafkaTransport(coroutineScope)
}
