package io.github.vooft.kafka.cluster

import io.github.vooft.kafka.network.NetworkClient

interface KafkaConnectionPoolFactory {
    fun create(): KafkaConnectionPool
}

class KafkaConnectionPoolFactoryImpl(private val networkClient: NetworkClient, private val nodesProvider: NodesProvider) : KafkaConnectionPoolFactory {
    override fun create(): KafkaConnectionPool {
        return KafkaConnectionPoolImpl(networkClient, nodesProvider)
    }
}
