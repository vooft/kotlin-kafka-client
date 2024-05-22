package io.github.vooft.kafka

import io.github.vooft.kafka.network.ktor.KtorNetworkClient
import io.github.vooft.kafka.network.messages.FetchRequestV4
import io.github.vooft.kafka.network.messages.FetchResponseV4
import io.github.vooft.kafka.network.messages.KafkaRecord
import io.github.vooft.kafka.network.messages.KafkaRecordBatch
import io.github.vooft.kafka.network.messages.KafkaRecordBatchBody
import io.github.vooft.kafka.network.messages.KafkaRecordBatchContainer
import io.github.vooft.kafka.network.messages.KafkaRecordBody
import io.github.vooft.kafka.network.messages.KafkaRecordHeader
import io.github.vooft.kafka.network.messages.OffsetFetchRequestV1
import io.github.vooft.kafka.network.messages.ProduceRequestV3
import io.github.vooft.kafka.network.sendRequest
import io.github.vooft.kafka.serialization.common.customtypes.toInt16String
import io.github.vooft.kafka.serialization.common.customtypes.toVarInt
import io.github.vooft.kafka.serialization.common.customtypes.toVarIntByteArray
import io.github.vooft.kafka.serialization.common.customtypes.toVarIntString
import io.github.vooft.kafka.serialization.common.customtypes.toVarLong
import io.github.vooft.kafka.serialization.decode
import kotlinx.coroutines.test.runTest
import kotlinx.io.Buffer
import kotlinx.io.readByteArray
import kotlin.test.Test

class KafkaTest {
    @Test
    fun test() = runTest {
        val ktorClient = KtorNetworkClient()

        Buffer().apply {
            writeInt(65540)
            println(readByteArray().toHexString())
        }

        val connection = ktorClient.connect("localhost", 9092)
//        val versionsResponse = connection.sendRequest<ApiVersionsRequestV1, ApiVersionsResponseV1>(ApiVersionsRequestV1)
//        println(versionsResponse)

//        val metadataResponse = connection.sendRequest<MetadataRequestV1, MetadataResponseV1>(MetadataRequestV1(listOf("test").toInt16String()))
//        println(metadataResponse)

        val produceRequest = ProduceRequestV3(
            topicData = listOf(
                ProduceRequestV3.TopicData(
                    name = "test".toInt16String(),
                    partitionData = listOf(
                        ProduceRequestV3.TopicData.PartitionData(
                            partitionIndex = 0,
                            batchContainer = KafkaRecordBatchContainer(
                                firstOffset = 0,
                                KafkaRecordBatch(
                                    body = KafkaRecordBatchBody(
                                        lastOffsetDelta = 1,
                                        firstTimestamp = 0,
                                        maxTimestamp = 0,
                                        records = listOf(
                                            KafkaRecord(
                                                recordBody = KafkaRecordBody(
//                                                    timestampDelta = 1024.toVarLong()
                                                    timestampDelta = 1024.toVarLong(),
                                                    offsetDelta = 0.toVarInt(),
                                                    recordKey = "test".encodeToByteArray().toVarIntByteArray(),
                                                    recordValue = "test1".encodeToByteArray().toVarIntByteArray(),
                                                    headers = listOf(
                                                        KafkaRecordHeader(
                                                            headerKey = "test".toVarIntString(),
                                                            headerValue = "test".toVarIntByteArray()
                                                        )
                                                    )
//                                                    offsetDelta = 0,
//                                                    key = "test".encodeToByteArray(),
//                                                    value = "test".encodeToByteArray()
                                                )
                                            ),
                                            KafkaRecord(
                                                recordBody = KafkaRecordBody(
//                                                    timestampDelta = 1024.toVarLong()
                                                    timestampDelta = 1024.toVarLong(),
                                                    offsetDelta = 1.toVarInt(),
                                                    recordKey = "test".encodeToByteArray().toVarIntByteArray(),
                                                    recordValue = "test2".encodeToByteArray().toVarIntByteArray(),
                                                    headers = listOf(
                                                        KafkaRecordHeader(
                                                            headerKey = "test".toVarIntString(),
                                                            headerValue = "test2".toVarIntByteArray()
                                                        )
                                                    )
//                                                    offsetDelta = 0,
//                                                    key = "test".encodeToByteArray(),
//                                                    value = "test".encodeToByteArray()
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

//        val produceResponse = connection.sendRequest<ProduceRequestV3, ProduceResponseV3>(produceRequest)
//        println(produceResponse)

        val offsetFetchRequest = OffsetFetchRequestV1(
            groupId = "test".toInt16String(),
            topics = listOf(
                OffsetFetchRequestV1.Topic(
                    topic = "test".toInt16String(),
                    partitions = listOf(
                        OffsetFetchRequestV1.Topic.Partition(
                            partition = 0
                        )
                    )
                )
            )
        )

//        val offsetFetchResponse = connection.sendRequest<OffsetFetchRequestV1, OffsetFetchResponseV1>(offsetFetchRequest)
//        println(offsetFetchResponse)

        val fetchRequest = FetchRequestV4(
            maxWaitTime = 1000,
            minBytes = 1,
            maxBytes = 1024,
            topics = listOf(
                FetchRequestV4.Topic(
                    topic = "test".toInt16String(),
                    partitions = listOf(
                        FetchRequestV4.Topic.Partition(
                            partition = 0,
                            fetchOffset = 0,
                            maxBytes = 128
                        )
                    )
                )
            )
        )

        val fetchResponse = connection.sendRequest<FetchRequestV4, FetchResponseV4>(fetchRequest)
        println(fetchResponse)

        connection.close()
    }

    @Test
    fun fff() {
        val data = byteArrayOf(
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            1,
            0,
            31,
            116,
            101,
            115,
            116,
            45,
            116,
            111,
            112,
            105,
            99,
            45,
            97,
            98,
            52,
            100,
            53,
            52,
            55,
            55,
            52,
            100,
            99,
            97,
            100,
            99,
            51,
            57,
            53,
            97,
            55,
            102,
            0,
            0,
            0,
            1,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            3,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            3,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            217.toByte(),
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            205.toByte(),
            0,
            0,
            0,
            0,
            2,
            214.toByte(),
            84,
            85,
            104,
            0,
            0,
            0,
            0,
            0,
            2,
            0,
            0,
            1,
            95,
            136.toByte(),
            193.toByte(),
            114,
            169.toByte(),
            0,
            0,
            1,
            95,
            136.toByte(),
            193.toByte(),
            114,
            169.toByte(),
            255.toByte(),
            255.toByte(),
            255.toByte(),
            255.toByte(),
            255.toByte(),
            255.toByte(),
            255.toByte(),
            255.toByte(),
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            3,
            102,
            0,
            0,
            0,
            10,
            107,
            101,
            121,
            45,
            48,
            24,
            115,
            111,
            109,
            101,
            45,
            118,
            97,
            108,
            117,
            101,
            45,
            48,
            2,
            24,
            104,
            101,
            97,
            100,
            101,
            114,
            45,
            107,
            101,
            121,
            45,
            48,
            28,
            104,
            101,
            97,
            100,
            101,
            114,
            45,
            118,
            97,
            108,
            117,
            101,
            45,
            48,
            102,
            0,
            0,
            2,
            10,
            107,
            101,
            121,
            45,
            49,
            24,
            115,
            111,
            109,
            101,
            45,
            118,
            97,
            108,
            117,
            101,
            45,
            49,
            2,
            24,
            104,
            101,
            97,
            100,
            101,
            114,
            45,
            107,
            101,
            121,
            45,
            49,
            28,
            104,
            101,
            97,
            100,
            101,
            114,
            45,
            118,
            97,
            108,
            117,
            101,
            45,
            49,
            102,
            0,
            0,
            4,
            10,
            107,
            101,
            121,
            45,
            50,
            24,
            115,
            111,
            109,
            101,
            45,
            118,
            97,
            108,
            117,
            101,
            45,
            50,
            2,
            24,
            104,
            101,
            97,
            100,
            101,
            114,
            45,
            107,
            101,
            121,
            45,
            50,
            28,
            104,
            101,
            97,
            100,
            101,
            114,
            45,
            118,
            97,
            108,
            117,
            101,
            45,
            50
        )

        println(data.toHexString())

        val buf = Buffer()
        buf.write(data)

        val response = buf.decode<FetchResponseV4>()
        println(response)

        val remaining = buf.readByteArray()
        require(remaining.isEmpty()) { "Buffer is not empty: ${remaining.toHexString()}" }
    }
}

private fun ByteArray.toHexString() = joinToString(", ", "[", "]") { "0x" + it.toUByte().toString(16).padStart(2, '0').uppercase() }
