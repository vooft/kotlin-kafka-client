package io.github.vooft.kafka.cluster

import io.github.vooft.kafka.network.messages.MetadataRequestV1
import io.github.vooft.kafka.network.messages.MetadataResponseV1
import io.github.vooft.kafka.network.sendRequest
import io.github.vooft.kafka.serialization.common.wrappers.BrokerAddress
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.NodeId
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant

interface KafkaNodesRegistry {
    suspend fun nodes(): Nodes
}

typealias Nodes = Map<NodeId, BrokerAddress>

class KafkaNodesRegistryImpl(private val connectionPool: KafkaConnectionPool, coroutineScope: CoroutineScope) : KafkaNodesRegistry {
    private val stateFlow: Deferred<MutableStateFlow<NodesState>> = coroutineScope.async {
        val nodes = fetchNodes()
        MutableStateFlow(NodesState(Clock.System.now(), nodes))
    }

    override suspend fun nodes(): Nodes {
        val state = stateFlow.await().value
        // TODO: update if needed
        return state.nodes
    }

    private suspend fun fetchNodes(): Nodes {
        val connection = connectionPool.acquire()
        val metadata = connection.sendRequest<MetadataRequestV1, MetadataResponseV1>(EMPTY_METADATA_REQUEST)

        return metadata.brokers.associateBy({ it.nodeId }, { BrokerAddress(it.host.value, it.port) })
    }

}

private data class NodesState(val updated: Instant, val nodes: Nodes)
private val EMPTY_METADATA_REQUEST = MetadataRequestV1(emptyList<KafkaTopic>())



