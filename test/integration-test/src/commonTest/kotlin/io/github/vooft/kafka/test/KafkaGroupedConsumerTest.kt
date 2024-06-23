package io.github.vooft.kafka.test

import io.github.vooft.kafka.KafkaDockerComposeConfig
import io.github.vooft.kafka.cluster.KafkaCluster
import io.github.vooft.kafka.consumer.KafkaRecord
import io.github.vooft.kafka.consumer.KafkaTopicConsumer
import io.github.vooft.kafka.producer.send
import io.github.vooft.kafka.serialization.common.wrappers.GroupId
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.ktuuid.UUID
import io.kotest.assertions.withClue
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.io.readString
import runIntegrationTest
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test

class KafkaGroupedConsumerTest {

    private val totalRecords = 100
    private val topic = KafkaTopic(UUID.randomUUID().toString())
    private val values = List(totalRecords) { UUID.randomUUID().toString() }

    private val group = GroupId(UUID.randomUUID().toString())

    private val cluster = KafkaCluster(KafkaDockerComposeConfig.bootstrapServers)

    @BeforeTest
    fun setUp() = runIntegrationTest {
        val producer = cluster.createProducer(topic)
        values.forEach { producer.send(it, it) }
    }

    @AfterTest
    fun tearDown() = runIntegrationTest {
        cluster.close()
    }

    @Test
    fun should_consume_using_2_consumers_in_a_group() = runIntegrationTest {
        val consumer1 = cluster.createConsumer(topic, group)
        val consumer2 = cluster.createConsumer(topic, group)

        var consumed1 = 0
        var consumed2 = 0

        val remaining = values.toMutableList()
        while (remaining.isNotEmpty()) {
            val batch1Deferred = async { consumer1.consume() }
            val batch2Deferred = async { consumer2.consume() }

            val batch1 = batch1Deferred.await()
            consumed1 += batch1.size

            val batch2 = batch2Deferred.await()
            consumed2 += batch2.size

            for (record in (batch1 + batch2)) {
                val value = record.value.readString()
                println("Received $value")
                remaining.remove(value)
            }

            println("Remaining: ${remaining.size}: $remaining")
        }

        println("Consumed by 1: $consumed1")
        println("Consumed by 2: $consumed2")
    }

    @Test
    fun should_consume_using_2_consumers_in_a_group_with_later_joining_3rd() = runIntegrationTest {
        val consumer1 = cluster.createConsumer(topic, group)
        val consumer2 = cluster.createConsumer(topic, group)
        var consumer3: Deferred<KafkaTopicConsumer>? = null

        var consumed1 = 0
        var consumed2 = 0
        var consumed3 = 0

        val remaining = values.toMutableList()
        while (remaining.isNotEmpty()) {

            val batch3Deferred: Deferred<List<KafkaRecord>> = when (consumer3) {
                null -> {
                    consumer3 = async(start = CoroutineStart.LAZY) { cluster.createConsumer(topic, group) }
                    async { listOf() }
                }

                else -> {
                    if (!consumer3.isCompleted) {
                        println("consumer3 joining")
                        consumer3.await()
                    }

                    async { consumer3.await().consume() }
                }
            }

            val batch1Deferred = async { consumer1.consume() }
            val batch2Deferred = async { consumer2.consume() }

            val batch1 = batch1Deferred.await()
            consumed1 += batch1.size

            val batch2 = batch2Deferred.await()
            consumed2 += batch2.size

            val batch3 = batch3Deferred.await()
            consumed3 += batch3.size

            for (record in (batch1 + batch2 + batch3)) {
                val value = record.value.readString()
                println("Received $value")
                remaining.remove(value)
            }

            println("Remaining: ${remaining.size}")
        }

        println("Consumed by 1: $consumed1")
        println("Consumed by 2: $consumed2")
        println("Consumed by 3: $consumed3")

        withClue("consumed1 = $consumed1, consumed2 = $consumed2, consumed3 = $consumed3, totalRecords = $totalRecords") {
            consumed1 + consumed2 + consumed3 shouldBe totalRecords
        }
    }
}
