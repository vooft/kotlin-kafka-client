package io.github.vooft.kafka.consumer

import io.github.vooft.kafka.cluster.KafkaCluster
import io.github.vooft.kafka.common.BrokerAddress
import io.github.vooft.kafka.common.GroupId
import io.github.vooft.kafka.common.KafkaTopic
import io.kotest.common.runBlocking
import kotlinx.coroutines.delay
import kotlin.random.Random
import kotlin.test.Test

class RebalanceTest {
    @OptIn(ExperimentalStdlibApi::class)
    @Test
    fun testRebalance() = runBlocking {
        val cluster = KafkaCluster(listOf(BrokerAddress("localhost", 9092)))

        val topic = Random.Default.nextBytes(10).toHexString()
        val group = Random.Default.nextBytes(10).toHexString()

        val consumer1 = cluster.createConsumer(KafkaTopic(topic), GroupId(group))
        delay(3000)

        val consumer2 = cluster.createConsumer(KafkaTopic(topic), GroupId(group))

        delay(99999999)
    }
}
