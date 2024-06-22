package io.github.vooft.kafka.manual
import io.github.vooft.kafka.network.NetworkClient
import io.github.vooft.kafka.network.ProduceRecord
import io.github.vooft.kafka.network.common.ErrorCode
import io.github.vooft.kafka.network.createDefaultClient
import io.github.vooft.kafka.network.metadata
import io.github.vooft.kafka.network.produce
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex
import kotlinx.coroutines.runBlocking
import kotlinx.io.Buffer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.testcontainers.kafka.KafkaContainer
import java.time.Duration
import java.util.Properties
import java.util.UUID

const val COUNT = 10
val topic = UUID.randomUUID().toString()

fun main() = runBlocking {
    KafkaContainer("apache/kafka").use { container ->
        container.withEnv("KAFKA_LOG_RETENTION_MS", "-1")
        container.withEnv("KAFKA_LOG_RETENTION_HOURS", "-1")
        container.withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")

        container.start()

        val client = NetworkClient.createDefaultClient()
        val connection = client.connect(container.host, container.firstMappedPort)
        do {
            val response = connection.metadata(topic)
            println("Metadata response: $response")
        } while (response.topics.single().errorCode != ErrorCode.NO_ERROR)


        repeat(COUNT) {
            val response = connection.produce(
                KafkaTopic(topic),
                mapOf(
                    PartitionIndex(0) to listOf(
                        ProduceRecord(
                            key = Buffer().apply { write("key $it".encodeToByteArray()) },
                            value = Buffer().apply { write("value $it".encodeToByteArray()) }
                        )
                    )
                )
            )

            println("Produced $it: ${response.topics.single().partitions.single().errorCode}")
        }

        val properties = Properties()
        properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = container.bootstrapServers
        properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        properties[ConsumerConfig.GROUP_ID_CONFIG] = UUID.randomUUID().toString()
        properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"

        var received = 0

        val consumer = KafkaConsumer<String, String>(properties)
        consumer.subscribe(listOf(topic))
        consumer.poll(Duration.ZERO)
        consumer.seekToBeginning(consumer.assignment())
        while (received < COUNT) {
            val pollResult = consumer.poll(Duration.ofSeconds(1))
            val records = pollResult.records(topic).toList()
            println("Received ${records.size} records")
            records.forEach { println("key: ${it.key()}, value: ${it.value()}") }

            received += records.size
        }

        println("done")
    }


}
