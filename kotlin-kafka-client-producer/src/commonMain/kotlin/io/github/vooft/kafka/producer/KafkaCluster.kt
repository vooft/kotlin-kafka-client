package io.github.vooft.kafka.producer

import io.github.vooft.kafka.common.BrokerAddress
import io.github.vooft.kafka.common.NodeId
import io.github.vooft.kafka.network.KafkaConnection
import io.github.vooft.kafka.network.ktor.KtorNetworkClient
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.async

class KafkaCluster(bootstrapServers: List<BrokerAddress>, private val coroutineScope: CoroutineScope = CoroutineScope(Job())) {

    private val networkClient = KtorNetworkClient()
    private val bootstrapConnections: List<Deferred<KafkaConnection>> = bootstrapServers.map {
        coroutineScope.async(start = CoroutineStart.LAZY) { networkClient.connect(it.hostname, it.port) }
    }

    private val metadataManager: KafkaMetadataManager = KafkaMetadataManagerImpl(bootstrapConnections, coroutineScope)
    private val connectionPool: KafkaConnectionPool = KafkaConnectionPoolImpl(networkClient, metadataManager)

    private val brokerConnections = mutableMapOf<NodeId, Deferred<KafkaConnection>>()

    suspend fun createProducer(topic: String): KafkaTopicProducer {
        val topicMetadata = metadataManager.queryTopicMetadata(topic)
        return SingleBrokerKafkaTopicProducer(topic, topicMetadata, connectionPool)
    }
}


