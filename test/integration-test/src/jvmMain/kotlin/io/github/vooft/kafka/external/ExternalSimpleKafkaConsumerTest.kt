package io.github.vooft.kafka.external

import io.github.vooft.kafka.cluster.KafkaCluster
import io.github.vooft.kafka.serialization.common.wrappers.BrokerAddress
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import kotlinx.coroutines.runBlocking
import kotlinx.io.readString
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import java.util.UUID

fun main() = runBlocking {
    val count = 10
    val topic = UUID.randomUUID().toString()

    val properties = Properties()
    properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
    properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name

    val producer = KafkaProducer<String, String>(properties)
    repeat(count) {
        val response = producer.send(ProducerRecord(topic, "key $it", "value $it"))
        println(response)
    }

    val cluster = KafkaCluster(listOf(BrokerAddress("localhost", 9092)), coroutineScope = this)
    val consumer = cluster.createConsumer(KafkaTopic(topic))

    var received = 0
    while (received < count) {
        val batch = consumer.consume()
        println("Received ${batch.size} records")
        batch.forEach { println("key: ${it.key.readString()}, value: ${it.value.readString()}, partition: ${it.partition}") }

        received += batch.size
    }

    println("done")


}
