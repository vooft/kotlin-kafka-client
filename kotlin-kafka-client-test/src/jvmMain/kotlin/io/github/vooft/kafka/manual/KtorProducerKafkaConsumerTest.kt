package io.github.vooft.kafka.manual
import io.github.vooft.kafka.common.KafkaTopic
import io.github.vooft.kafka.common.PartitionIndex
import io.github.vooft.kafka.network.common.toVarInt
import io.github.vooft.kafka.network.common.toVarIntByteArray
import io.github.vooft.kafka.network.ktor.KtorNetworkClient
import io.github.vooft.kafka.network.messages.ErrorCode
import io.github.vooft.kafka.network.messages.KafkaRecordBatchContainerV0
import io.github.vooft.kafka.network.messages.KafkaRecordV0
import io.github.vooft.kafka.network.messages.MetadataRequestV1
import io.github.vooft.kafka.network.messages.MetadataResponseV1
import io.github.vooft.kafka.network.messages.ProduceRequestV3
import io.github.vooft.kafka.network.messages.ProduceResponseV3
import io.github.vooft.kafka.network.sendRequest
import io.github.vooft.kafka.serialization.common.primitives.VarIntBytesSizePrefixed
import kotlinx.coroutines.runBlocking
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

        val client = KtorNetworkClient()
        val connection = client.connect(container.host, container.firstMappedPort)
        do {
            val response = connection.sendRequest<MetadataRequestV1, MetadataResponseV1>(MetadataRequestV1(topic))
            println("Metadata response: $response")
        } while (response.topics.single().errorCode != ErrorCode.NO_ERROR)


        repeat(COUNT) {
            val response = connection.sendRequest<ProduceRequestV3, ProduceResponseV3>(
                ProduceRequestV3(
                    topicData = listOf(
                        ProduceRequestV3.TopicData(
                            topic = KafkaTopic(topic),
                            partitionData = listOf(
                                ProduceRequestV3.TopicData.PartitionData(
                                    partition = PartitionIndex(0),
                                    batchContainer = KafkaRecordBatchContainerV0(
                                        batch = KafkaRecordBatchContainerV0.KafkaRecordBatch(
                                            body = KafkaRecordBatchContainerV0.KafkaRecordBatch.KafkaRecordBatchBody(
                                                lastOffsetDelta = 0,
                                                firstTimestamp = System.currentTimeMillis(),
                                                maxTimestamp = System.currentTimeMillis(),
                                                records = listOf(
                                                    KafkaRecordV0(
                                                        recordBody = VarIntBytesSizePrefixed(
                                                            KafkaRecordV0.KafkaRecordBody(
                                                                offsetDelta = 0.toVarInt(),
                                                                recordKey = "key $it".toVarIntByteArray(),
                                                                recordValue = "value $it".toVarIntByteArray()
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )

            println("Produced $it: ${response.topicResponses.single().partitionResponses.single().errorCode}")
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
        consumer.poll(0)
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
