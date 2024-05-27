package io.github.vooft.kafka.external

import io.github.vooft.kafka.cluster.KafkaCluster
import io.github.vooft.kafka.common.BrokerAddress
import kotlinx.coroutines.runBlocking
import kotlinx.io.readString

fun main() = runBlocking {
    val count = 10
//    val topic = UUID.randomUUID().toString()
    val topic = "test123"
//    val groupId = UUID.randomUUID()
    val groupId = "ca09ae6b-0f1f-493d-8905-4b59934e84ea"

//    val properties = Properties()
//    properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
//    properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
//    properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
//
//    val producer = KafkaProducer<String, String>(properties)
//    repeat(count) {
//        val response = producer.send(ProducerRecord(topic, "key $it", "value $it"))
//        println(response)
//    }

    val cluster = KafkaCluster(listOf(BrokerAddress("localhost", 9092)), coroutineScope = this)
    val consumer = cluster.createConsumer(topic, groupId)

    var received = 0
    while (received < count) {
        val batch = consumer.consume()
        println("Received ${batch.size} records")
        batch.forEach { println("key: ${it.key.readString()}, value: ${it.value.readString()}, partition: ${it.partition}") }

        received += batch.size
    }

    println("done")


}
