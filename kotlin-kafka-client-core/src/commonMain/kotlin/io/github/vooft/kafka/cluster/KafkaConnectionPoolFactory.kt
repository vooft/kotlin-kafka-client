package io.github.vooft.kafka.cluster

import io.github.vooft.kafka.network.KafkaTransport

interface KafkaConnectionPoolFactory {
    fun create(): KafkaConnectionPool
}

class KafkaConnectionPoolFactoryImpl(
    private val transport: KafkaTransport,
    private val nodesRegistry: KafkaNodesRegistry
) : KafkaConnectionPoolFactory {
    override fun create(): KafkaConnectionPool {
        return KafkaDynamicNodesListConnectionPool(transport, nodesRegistry)
    }
}
