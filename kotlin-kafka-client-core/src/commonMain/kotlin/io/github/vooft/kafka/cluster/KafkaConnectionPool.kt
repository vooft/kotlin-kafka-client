package io.github.vooft.kafka.cluster

import io.github.vooft.kafka.common.types.BrokerAddress
import io.github.vooft.kafka.common.types.NodeId
import io.github.vooft.kafka.transport.KafkaConnection
import io.github.vooft.kafka.transport.KafkaTransport
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

interface KafkaConnectionPool {
    suspend fun acquire(nodeId: NodeId? = null): KafkaConnection
}

class KafkaDynamicNodesListConnectionPool(
    private val transport: KafkaTransport,
    private val nodesRegistry: KafkaNodesRegistry
) : KafkaConnectionPool {

    private val connections = mutableMapOf<BrokerAddress, KafkaConnection>()
    private val connectionsMutex = Mutex()

    // TODO: make it more reactive and update when metadata changes
    override suspend fun acquire(nodeId: NodeId?): KafkaConnection {
        // remove closed connections first
        connectionsMutex.withLock {
            val closed = connections.filterValues { !it.isClosed }
            closed.keys.forEach { connections.remove(it) }
        }

        val nodes = nodesRegistry.nodes()
        val brokerAddress = nodeId?.let { nodes.getValue(it) } ?: nodes.values.random()
        return connectionsMutex.withLock {
            connections.getOrPut(brokerAddress) {
                transport.connect(brokerAddress.hostname, brokerAddress.port)
            }
        }
    }
}

class KafkaFixedNodesListConnectionPool(
    private val transport: KafkaTransport,
    nodes: List<BrokerAddress>,
    coroutineScope: CoroutineScope
) : KafkaConnectionPool {

    private val connectionsDeferred = coroutineScope.async {
        nodes.map { BrokerConnection(it, MutableStateFlow(transport.connect(it.hostname, it.port))) }
    }
    private val connectionsMutex = Mutex()

    override suspend fun acquire(nodeId: NodeId?): KafkaConnection {
        require(nodeId == null) { "nodeId is not supported for ${this::class}" }
        val connections = connectionsDeferred.await()
        val connection = connections.random()

        if (connection.isClosed) {
            connectionsMutex.withLock {
                if (connection.isClosed) {
                    connection.value = transport.connect(connection.address.hostname, connection.address.port)
                }
            }
        }

        return connection.value
    }
}

private data class BrokerConnection(val address: BrokerAddress, val connection: MutableStateFlow<KafkaConnection>) :
    MutableStateFlow<KafkaConnection> by connection

private val BrokerConnection.isClosed: Boolean get() = connection.value.isClosed
