package io.github.vooft.kafka.cluster

import io.github.vooft.kafka.network.KafkaConnection
import io.github.vooft.kafka.network.NetworkClient
import io.github.vooft.kafka.serialization.common.wrappers.BrokerAddress
import io.github.vooft.kafka.serialization.common.wrappers.NodeId
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

interface KafkaConnectionPool {
    suspend fun acquire(nodeId: NodeId? = null): KafkaConnection
}

class KafkaDynamicNodesListConnectionPool(
    private val networkClient: NetworkClient,
    private val nodesRegistry: KafkaNodesRegistry
) : KafkaConnectionPool {

    private val connections = mutableMapOf<BrokerAddress, KafkaConnection>()
    private val connectionsMutex = Mutex()

    // TODO: make it more reactive and update when metadata changes
    override suspend fun acquire(nodeId: NodeId?): KafkaConnection {
        val nodes = nodesRegistry.nodes()
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

class KafkaFixedNodesListConnectionPool(
    networkClient: NetworkClient,
    nodes: List<BrokerAddress>,
    coroutineScope: CoroutineScope
) : KafkaConnectionPool {

    private val connections = nodes.associateWith {
        coroutineScope.async { networkClient.connect(it.hostname, it.port) }
    }

    override suspend fun acquire(nodeId: NodeId?): KafkaConnection {
        require(nodeId == null) { "nodeId is not supported for KafkaFixedBrokerListConnectionPool" }
        return connections.values.random().await()
    }

}
