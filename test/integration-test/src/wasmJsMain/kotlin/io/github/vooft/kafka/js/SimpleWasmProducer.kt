package io.github.vooft.kafka.js

import io.github.oshai.kotlinlogging.KotlinLoggingConfiguration
import io.github.oshai.kotlinlogging.Level
import io.github.vooft.kafka.cluster.KafkaCluster
import io.github.vooft.kafka.producer.send
import io.github.vooft.kafka.serialization.common.wrappers.BrokerAddress
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import kotlinx.coroutines.delay

suspend fun main() {
    println("starting simple js producer")

    try {
        KotlinLoggingConfiguration.logLevel = Level.TRACE

        val count = 10
        val topic = "my-topic"

        println("creating cluster")
        val cluster = KafkaCluster(listOf(BrokerAddress("localhost", 9092)))

        println("creating producer")
        val producer = cluster.createProducer(KafkaTopic(topic))

        println("sending")
        repeat(count) {
            println("trying to send $it")
            val response = producer.send("key $it", "value $it")
            println(response)
        }
    } catch (e: Exception) {
        @Suppress("detekt:PrintStackTrace")
        e.printStackTrace()
    }


    delay(999999)
}
