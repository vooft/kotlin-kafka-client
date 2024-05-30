package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.common.KafkaTopic
import io.github.vooft.kafka.common.NodeId
import io.github.vooft.kafka.common.PartitionIndex
import io.github.vooft.kafka.network.messages.MetadataResponseV1.Broker
import io.github.vooft.kafka.network.messages.MetadataResponseV1.Topic
import io.github.vooft.kafka.network.messages.MetadataResponseV1.Topic.Partition
import io.github.vooft.kafka.serialization.KafkaSerde
import io.github.vooft.kafka.serialization.common.customtypes.Int16String
import io.github.vooft.kafka.serialization.common.customtypes.NullableInt16String
import io.github.vooft.kafka.serialization.common.primitives.Int32List
import io.github.vooft.kafka.serialization.decode
import io.github.vooft.kafka.serialization.encode
import io.kotest.matchers.shouldBe
import kotlinx.io.Buffer
import kotlinx.io.readByteArray
import kotlin.test.Test

class MetadataTest {

    private val topics = listOf("topic1", "topic2")
    private val request = MetadataRequestV1(topics.map { KafkaTopic(it) })
    private val encodedRequest = byteArrayOf(
        0x0, 0x0, 0x0, 0x2, // int32 collection topics.size

        0x0, 0x6, // int16 string length
        0x74, 0x6f, 0x70, 0x69, 0x63, 0x31, // topic1

        0x0, 0x6, // int16 string length
        0x74, 0x6f, 0x70, 0x69, 0x63, 0x32 // topic1
    )

    private val partition = Partition(
        errorCode = ErrorCode.NO_ERROR,
        partition = PartitionIndex(10),
        leader = NodeId(100),
        replicas = Int32List(100, 200),
        isr = Int32List(100, 200)
    )

    private val encodedPartition = byteArrayOf(
        0x0, 0x0, // int16 error code

        0x0, 0x0, 0x0, 0xA, // int32 partition index

        0x0, 0x0, 0x0, 0x64, // int32 leader

        0x0, 0x0, 0x0, 0x2, // int32 collection replicas.size

        0x0, 0x0, 0x0, 0x64, // int32 replica 1 = 100
        0x0, 0x0, 0x0, 0xC8.toByte(), // int32 replica 2 = 200

        0x0, 0x0, 0x0, 0x2, // int32 collection isr.size
        0x0, 0x0, 0x0, 0x64, // int32 isr 1
        0x0, 0x0, 0x0, 0xC8.toByte() // int32 isr 2
    )

    private val topic = Topic(
        errorCode = ErrorCode.NO_ERROR,
        topic = KafkaTopic("topic1"),
        isInternal = false,
        partitions = Int32List(partition)
    )
    private val encodedTopic = byteArrayOf(
        0x0, 0x0, // int16 error code

        0x0, 0x6, // int16 string length
        0x74, 0x6f, 0x70, 0x69, 0x63, 0x31, // topic1

        0x0, // boolean isInternal

        0x0, 0x0, 0x0, 0x1, // int32 collection partitions.size

        *encodedPartition
    )

    private val broker = Broker(
        nodeId = NodeId(100),
        host = Int16String("localhost"),
        port = 9092,
        rack = NullableInt16String("rack1")
    )
    private val encodedBroker = byteArrayOf(
        0x0, 0x0, 0x0, 0x64, // int32 node id

        0x0, 0x9, // int16 string length
        0x6C, 0x6F, 0x63, 0x61, 0x6C, 0x68, 0x6F, 0x73, 0x74, // localhost

        0x0, 0x0, 0x23, 0x84.toByte(), // int32 port 9092

        0x0, 0x5, // int16 string length
        0x72, 0x61, 0x63, 0x6B, 0x31 // rack1
    )

    private val response = MetadataResponseV1(
        brokers = Int32List(broker),
        controllerId = 100,
        topics = Int32List(topic)
    )
    private val encodedResponse = byteArrayOf(
        0x0, 0x0, 0x0, 0x1, // int32 collection brokers.size

        *encodedBroker,

        0x0, 0x0, 0x0, 0x64, // int32 controller id

        0x0, 0x0, 0x0, 0x1, // int32 collection topics.size

        *encodedTopic
    )

    @Test
    fun should_serialize_metadata_request() {
        val actual = KafkaSerde.encode(request)
        actual.readByteArray() shouldBe encodedRequest
    }

    @Test
    fun should_deserialize_metadata_request() {
        val actual = KafkaSerde.decode<MetadataRequestV1>(Buffer().apply { write(encodedRequest) })
        actual shouldBe request
    }

    @Test
    fun should_serialize_response_partition() {
        val actual = KafkaSerde.encode(partition)
        actual.readByteArray() shouldBe encodedPartition
    }

    @Test
    fun should_deserialize_response_partition() {
        val actual = KafkaSerde.decode<Partition>(Buffer().apply { write(encodedPartition) })
        actual shouldBe partition
    }

    @Test
    fun should_serialize_response_topic() {
        val actual = KafkaSerde.encode(topic)
        actual.readByteArray() shouldBe encodedTopic
    }

    @Test
    fun should_deserialize_response_topic() {
        val actual = KafkaSerde.decode<Topic>(Buffer().apply { write(encodedTopic) })
        actual shouldBe topic
    }

    @Test
    fun should_serialize_response_broker() {
        val actual = KafkaSerde.encode(broker)
        actual.readByteArray() shouldBe encodedBroker
    }

    @Test
    fun should_deserialize_response_broker() {
        val actual = KafkaSerde.decode<Broker>(Buffer().apply { write(encodedBroker) })
        actual shouldBe broker
    }

    @Test
    fun should_serialize_response() {
        val actual = KafkaSerde.encode(response)
        actual.readByteArray() shouldBe encodedResponse
    }

    @Test
    fun should_deserialize_response() {
        val actual = KafkaSerde.decode<MetadataResponseV1>(Buffer().apply { write(encodedResponse) })
        actual shouldBe response
    }
}
