package io.github.vooft.kafka.consumer

import io.github.vooft.kafka.cluster.KafkaCluster
import io.github.vooft.kafka.serialization.common.wrappers.BrokerAddress
import io.github.vooft.kafka.serialization.common.wrappers.GroupId
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.utils.toHexString
import io.kotest.common.runBlocking
import kotlinx.coroutines.delay
import kotlin.random.Random

class RebalanceTest {
//    @Test
    fun testRebalance() = runBlocking {
        val cluster = KafkaCluster(listOf(BrokerAddress("localhost", 9092)))

        val topic = Random.Default.nextBytes(10).toHexString()
        val group = Random.Default.nextBytes(10).toHexString()

        val consumer1 = cluster.createConsumer(KafkaTopic(topic), GroupId(group))
        println(consumer1)
        delay(3000)

        val consumer2 = cluster.createConsumer(KafkaTopic(topic), GroupId(group))
        println(consumer2)

        delay(99999999)
    }
}
