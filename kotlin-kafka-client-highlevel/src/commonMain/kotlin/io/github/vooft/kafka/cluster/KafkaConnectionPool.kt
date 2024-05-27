package io.github.vooft.kafka.cluster

import io.github.vooft.kafka.common.BrokerAddress
import io.github.vooft.kafka.common.NodeId
import io.github.vooft.kafka.network.KafkaConnection
import io.github.vooft.kafka.network.NetworkClient
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

interface KafkaConnectionPool {
    suspend fun acquire(nodeId: NodeId? = null): KafkaConnection
}

class KafkaConnectionPoolImpl(
    private val networkClient: NetworkClient,
    private val nodesProvider: NodesProvider
) : KafkaConnectionPool {

    private val connections = mutableMapOf<BrokerAddress, KafkaConnection>()
    private val connectionsMutex = Mutex()

    // TODO: make it more reactive and update when metadata changes
    override suspend fun acquire(nodeId: NodeId?): KafkaConnection {
        val nodes = nodesProvider.nodes()
        val brokerAddress = nodeId?.let { nodes.getValue(it) } ?: nodes.values.random()
        connectionsMutex.withLock {
            val existing = connections[brokerAddress]
            if (existing != null) {
                return existing
            } else {
                val connection = networkClient.connect(brokerAddress.hostname, brokerAddress.port)
                connections[brokerAddress] = connection
                return connection
            }
        }
    }

}
