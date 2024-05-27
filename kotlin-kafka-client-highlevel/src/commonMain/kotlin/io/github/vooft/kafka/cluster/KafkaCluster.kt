package io.github.vooft.kafka.cluster

import io.github.vooft.kafka.common.BrokerAddress
import io.github.vooft.kafka.common.GroupId
import io.github.vooft.kafka.common.KafkaTopic
import io.github.vooft.kafka.consumer.KafkaTopicConsumer
import io.github.vooft.kafka.consumer.SimpleKafkaTopicConsumer
import io.github.vooft.kafka.consumer.group.KafkaConsumerGroupManager
import io.github.vooft.kafka.network.KafkaConnection
import io.github.vooft.kafka.network.ktor.KtorNetworkClient
import io.github.vooft.kafka.producer.KafkaTopicProducer
import io.github.vooft.kafka.producer.SimpleKafkaTopicProducer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class KafkaCluster(bootstrapServers: List<BrokerAddress>, private val coroutineScope: CoroutineScope = CoroutineScope(Job())) {

    private val networkClient = KtorNetworkClient()
    private val bootstrapConnections: List<Deferred<KafkaConnection>> = bootstrapServers.map {
        coroutineScope.async(start = CoroutineStart.LAZY) { networkClient.connect(it.hostname, it.port) }
    }

    private val metadataManager: KafkaMetadataManager = KafkaMetadataManagerImpl(bootstrapConnections, coroutineScope)
    private val connectionPool: KafkaConnectionPool = KafkaConnectionPoolImpl(networkClient, metadataManager.nodesProvider())

    private val consumerGroupManagers = mutableMapOf<TopicGroup, KafkaConsumerGroupManager>()
    private val consumerGroupManagersMutex = Mutex()

    suspend fun createProducer(topic: KafkaTopic): KafkaTopicProducer {
        val topicMetadataProvider = metadataManager.topicMetadataProvider(topic)
        return SimpleKafkaTopicProducer(topic, topicMetadataProvider, connectionPool)
    }

    suspend fun createConsumer(topic: KafkaTopic, groupId: GroupId? = null): KafkaTopicConsumer {
        if (groupId == null) {
            val topicMetadataProvider = metadataManager.topicMetadataProvider(topic)
            return SimpleKafkaTopicConsumer(topicMetadataProvider, connectionPool, coroutineScope)
        } else {
            val consumerGroupManager = consumerGroupManagersMutex.withLock {
                consumerGroupManagers.getOrPut(TopicGroup(topic, groupId)) {
                    KafkaConsumerGroupManager(topic, groupId, metadataManager, connectionPool, coroutineScope)
                }
            }

            val topicMetadataProvider = consumerGroupManager.nextGroupedTopicMetadataProvider()
            return SimpleKafkaTopicConsumer(topicMetadataProvider, connectionPool, coroutineScope)
        }
    }
}

data class TopicGroup(val topic: KafkaTopic, val groupId: GroupId)


