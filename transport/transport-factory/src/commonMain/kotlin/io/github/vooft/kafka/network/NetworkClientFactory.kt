package io.github.vooft.kafka.network

import kotlinx.coroutines.CoroutineScope

expect fun KafkaTransport.Companion.createDefaultClient(coroutineScope: CoroutineScope): KafkaTransport
