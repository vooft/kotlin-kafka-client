package io.github.vooft.kafka.cluster

import io.github.vooft.kafka.consumer.KafkaTopicConsumer
import io.github.vooft.kafka.consumer.SimpleKafkaTopicConsumer
import io.github.vooft.kafka.consumer.group.KafkaConsumerGroupManager
import io.github.vooft.kafka.network.KafkaTransport
import io.github.vooft.kafka.network.createDefaultClient
import io.github.vooft.kafka.producer.KafkaTopicProducer
import io.github.vooft.kafka.producer.SimpleKafkaTopicProducer
import io.github.vooft.kafka.serialization.common.wrappers.BrokerAddress
import io.github.vooft.kafka.serialization.common.wrappers.GroupId
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class KafkaCluster(bootstrapServers: List<BrokerAddress>, private val coroutineScope: CoroutineScope = CoroutineScope(Job())) {

    private val transport = KafkaTransport.createDefaultClient()
    private val bootstrapConnectionPool: KafkaConnectionPool = KafkaFixedNodesListConnectionPool(
        transport = transport,
        nodes = bootstrapServers,
        coroutineScope = coroutineScope
    )

    private val nodesRegistry: KafkaNodesRegistry = KafkaNodesRegistryImpl(bootstrapConnectionPool, coroutineScope)
    private val topicRegistry: KafkaClusterTopicsRegistry = KafkaClusterTopicsRegistryImpl(bootstrapConnectionPool)
    private val connectionPoolFactory: KafkaConnectionPoolFactory = KafkaConnectionPoolFactoryImpl(transport, nodesRegistry)

    private val consumerGroupManagers = mutableMapOf<TopicGroup, KafkaConsumerGroupManager>()
    private val consumerGroupManagersMutex = Mutex()

    suspend fun createProducer(topic: KafkaTopic): KafkaTopicProducer {
        val topicStateProvider = topicRegistry.forTopic(topic)
        return SimpleKafkaTopicProducer(topic, topicStateProvider, connectionPoolFactory.create())
    }

    suspend fun createConsumer(topic: KafkaTopic, groupId: GroupId? = null): KafkaTopicConsumer {
        val topicStateProvider = topicRegistry.forTopic(topic)

        if (groupId == null) {
            return SimpleKafkaTopicConsumer(
                topicStateProvider = topicStateProvider,
                connectionPool = connectionPoolFactory.create(),
                coroutineScope = coroutineScope)
        } else {
            val consumerGroupManager = consumerGroupManagersMutex.withLock {
                consumerGroupManagers.getOrPut(TopicGroup(topic, groupId)) {
                    KafkaConsumerGroupManager(
                        topic = topic,
                        groupId = groupId,
                        topicStateProvider = topicStateProvider,
                        connectionPoolFactory = connectionPoolFactory,
                        coroutineScope = coroutineScope
                    )
                }
            }

            return consumerGroupManager.createConsumer()
        }
    }
}

data class TopicGroup(val topic: KafkaTopic, val groupId: GroupId)


