package io.github.vooft.kafka.test

import io.github.vooft.kafka.KafkaDockerComposeConfig
import io.github.vooft.kafka.cluster.KafkaCluster
import io.github.vooft.kafka.producer.send
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.io.readString
import kotlinx.uuid.UUID
import kotlinx.uuid.generateUUID
import kotlin.test.Test

class KafkaSimpleTest {

    private val totalRecords = 100
    private val topic = KafkaTopic(UUID.generateUUID().toString())
    private val values = List(totalRecords) { it.toString() }

    @Test
    fun should_produce_and_consume_message() = runTest {
        val cluster = KafkaCluster(KafkaDockerComposeConfig.bootstrapServers)
        try {
            println("creating producer")
            val producer = cluster.createProducer(topic)

            println("send dummy values")
            repeat(10) {
                val value = (0 - it).toString()
                producer.send(value, value)
                delay(50)
            }

            println("sending values")
            values.forEach {
                producer.send(it, it)
                println("Sent $it")
            }

            val consumer = cluster.createConsumer(topic)

            val remaining = values.toMutableList()
            while (remaining.isNotEmpty()) {
                val batch = consumer.consume()
                for (record in batch) {
                    val value = record.value.readString()
                    println("Received $value")
                    remaining.remove(value)
                }

                println("Remaining: ${remaining.size}: $remaining")
            }
        } finally {
            cluster.close()
        }
    }
}
