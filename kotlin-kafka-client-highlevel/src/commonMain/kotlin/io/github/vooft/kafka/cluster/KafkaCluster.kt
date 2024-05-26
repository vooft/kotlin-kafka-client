package io.github.vooft.kafka.cluster

import io.github.vooft.kafka.common.BrokerAddress
import io.github.vooft.kafka.consumer.KafkaTopicConsumer
import io.github.vooft.kafka.consumer.SimpleKafkaTopicConsumer
import io.github.vooft.kafka.network.KafkaConnection
import io.github.vooft.kafka.network.ktor.KtorNetworkClient
import io.github.vooft.kafka.producer.KafkaTopicProducer
import io.github.vooft.kafka.producer.SimpleKafkaTopicProducer
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
    private val connectionPool: KafkaConnectionPool = KafkaConnectionPoolImpl(networkClient, metadataManager.nodesProvider())

    suspend fun createProducer(topic: String): KafkaTopicProducer {
        val topicMetadataProvider = metadataManager.topicMetadataProvider(topic)
        return SimpleKafkaTopicProducer(topic, topicMetadataProvider, connectionPool)
    }

    suspend fun createConsumer(topic: String): KafkaTopicConsumer {
        val topicMetadataProvider = metadataManager.topicMetadataProvider(topic)
        return SimpleKafkaTopicConsumer(topic, topicMetadataProvider, connectionPool, coroutineScope)
    }
}


