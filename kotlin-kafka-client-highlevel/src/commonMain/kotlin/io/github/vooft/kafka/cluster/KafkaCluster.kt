package io.github.vooft.kafka.cluster

import io.github.vooft.kafka.common.BrokerAddress
import io.github.vooft.kafka.common.GroupId
import io.github.vooft.kafka.common.KafkaTopic
import io.github.vooft.kafka.consumer.KafkaTopicConsumer
import io.github.vooft.kafka.consumer.SimpleKafkaTopicConsumer
import io.github.vooft.kafka.consumer.group.KafkaConsumerGroupManager
import io.github.vooft.kafka.network.ktor.KtorNetworkClient
import io.github.vooft.kafka.producer.KafkaTopicProducer
import io.github.vooft.kafka.producer.SimpleKafkaTopicProducer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class KafkaCluster(bootstrapServers: List<BrokerAddress>, private val coroutineScope: CoroutineScope = CoroutineScope(Job())) {

    private val networkClient = KtorNetworkClient()
    private val bootstrapConnectionPool: KafkaConnectionPool = KafkaFixedNodesListConnectionPool(
        networkClient = networkClient,
        nodes = bootstrapServers,
        coroutineScope = coroutineScope
    )

    private val nodesRegistry: KafkaNodesRegistry = KafkaNodesRegistryImpl(bootstrapConnectionPool, coroutineScope)
    private val topicRegistry: KafkaClusterTopicsRegistry = KafkaClusterTopicsRegistryImpl(bootstrapConnectionPool)
    private val connectionPoolFactory: KafkaConnectionPoolFactory = KafkaConnectionPoolFactoryImpl(networkClient, nodesRegistry)

    private val consumerGroupManagers = mutableMapOf<TopicGroup, KafkaConsumerGroupManager>()
    private val consumerGroupManagersMutex = Mutex()

    suspend fun createProducer(topic: KafkaTopic): KafkaTopicProducer {
        val topicStateProvider = topicRegistry.forTopic(topic)
        return SimpleKafkaTopicProducer(topic, topicStateProvider, connectionPoolFactory.create())
    }

    suspend fun createConsumer(topic: KafkaTopic, groupId: GroupId? = null): KafkaTopicConsumer {
        val topicStateProvider = topicRegistry.forTopic(topic)

        if (groupId == null) {
            return SimpleKafkaTopicConsumer(topicStateProvider, connectionPoolFactory.create(), coroutineScope)
        } else {
            val consumerGroupManager = consumerGroupManagersMutex.withLock {
                consumerGroupManagers.getOrPut(TopicGroup(topic, groupId)) {
                    KafkaConsumerGroupManager(topic, groupId, topicStateProvider, connectionPoolFactory, coroutineScope)
                }
            }

            return consumerGroupManager.createConsumer()
        }
    }
}

data class TopicGroup(val topic: KafkaTopic, val groupId: GroupId)


