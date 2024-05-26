package io.github.vooft.kafka.cluster

import io.github.vooft.kafka.common.BrokerAddress
import io.github.vooft.kafka.common.NodeId
import io.github.vooft.kafka.network.KafkaConnection
import io.github.vooft.kafka.network.NetworkClient
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

interface KafkaConnectionPool {
    suspend fun acquire(nodeId: NodeId): KafkaConnection
}

class KafkaConnectionPoolImpl(
    private val networkClient: NetworkClient,
    private val kafkaMetadataManager: KafkaMetadataManager
) : KafkaConnectionPool {

    private val connections = mutableMapOf<BrokerAddress, KafkaConnection>()
    private val connectionsMutex = Mutex()

    // TODO: make it more reactive and update when metadata changes
    override suspend fun acquire(nodeId: NodeId): KafkaConnection {
        val nodes = kafkaMetadataManager.queryLatestNodes()
        val brokerAddress = nodes.getValue(nodeId)
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
