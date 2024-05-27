package io.github.vooft.kafka.external

import io.github.vooft.kafka.cluster.KafkaCluster
import io.github.vooft.kafka.common.BrokerAddress
import io.github.vooft.kafka.common.KafkaTopic
import io.github.vooft.kafka.producer.send
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties
import java.util.UUID

fun main() = runBlocking {

    val count = 10
    val topic = UUID.randomUUID().toString()

    val cluster = KafkaCluster(listOf(BrokerAddress("localhost", 9092)), coroutineScope = this)
    val producer = cluster.createProducer(KafkaTopic(topic))

    repeat(count) {
        val response = producer.send("key $it", "value $it")
        println(response)
    }

    val properties = Properties()
    properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
    properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
    properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
    properties[ConsumerConfig.GROUP_ID_CONFIG] = UUID.randomUUID().toString()
    properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

    var received = 0

    val consumer = KafkaConsumer<String, String>(properties)
    consumer.subscribe(listOf(topic))
    consumer.poll(0)
    consumer.seekToBeginning(consumer.assignment())
    while (received < count) {
        val pollResult = consumer.poll(Duration.ofSeconds(1))
        val records = pollResult.records(topic).toList()
        println("Received ${records.size} records")
        records.forEach { println("key: ${it.key()}, value: ${it.value()}, partition: ${it.partition()}") }

        received += records.size
    }

    println("done")


}
