package io.github.vooft.kafka

import io.github.vooft.kafka.common.types.BrokerAddress

object KafkaDockerComposeConfig {
    val bootstrapServers = listOf(
        BrokerAddress("localhost", 9092),
        BrokerAddress("localhost", 9093),
//        BrokerAddress("localhost", 9094),
    )
}
