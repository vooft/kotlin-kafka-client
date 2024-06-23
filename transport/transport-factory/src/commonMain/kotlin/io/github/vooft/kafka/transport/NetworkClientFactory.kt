package io.github.vooft.kafka.transport

import kotlinx.coroutines.CoroutineScope

expect fun KafkaTransport.Companion.createDefaultClient(coroutineScope: CoroutineScope): KafkaTransport
