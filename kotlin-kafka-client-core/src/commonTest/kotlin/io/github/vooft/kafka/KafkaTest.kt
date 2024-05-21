package io.github.vooft.kafka

import io.github.vooft.kafka.network.ktor.KtorNetworkClient
import io.github.vooft.kafka.network.messages.KafkaRecord
import io.github.vooft.kafka.network.messages.KafkaRecordBatch
import io.github.vooft.kafka.network.messages.KafkaRecordBatchBody
import io.github.vooft.kafka.network.messages.KafkaRecordBatchContainer
import io.github.vooft.kafka.network.messages.KafkaRecordBody
import io.github.vooft.kafka.network.messages.ProduceRequestV3
import io.github.vooft.kafka.network.messages.ProduceResponseV1
import io.github.vooft.kafka.network.sendRequest
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

//        val metadataResponse = connection.sendRequest<MetadataRequestV1, MetadataResponseV1>(MetadataRequestV1(listOf("test")))
//        println(metadataResponse)

        val produceRequest = ProduceRequestV3(
            topicData = listOf(
                ProduceRequestV3.TopicData(
                    name = "test",
                    partitionData = listOf(
                        ProduceRequestV3.TopicData.PartitionData(
                            partitionIndex = 0,
                            batchContainer = KafkaRecordBatchContainer(
                                firstOffset = 0,
                                KafkaRecordBatch(
                                    body = KafkaRecordBatchBody(
                                        lastOffsetDelta = 0,
                                        firstTimestamp = 0,
                                        maxTimestamp = 0,
                                        records = listOf(
                                            KafkaRecord(
                                                recordBody = KafkaRecordBody(
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

        // kafkajs
        // 0xFF, 0xFF, // transactionalId = null
        // 0xFF, 0xFF, // acks = -1
        // 0x00, 0x00, 0x03, 0xE8, // timeoutMs = 1000
        // 0x00, 0x00, 0x00, 0x01, // topicData length = 1
        // 0x00, 0x04, // name length "test
        // 0x74, 0x65, 0x73, 0x74, // "test"
        // 0x00, 0x00, 0x00, 0x01, // partitionData length
        // 0x00, 0x00, 0x00, 0x00, // partitionIndex
        // 0x00, 0x00, 0x00, 0x46, // kafka records batch container size in bytes
        // 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // first offset = 0
        // 0x00, 0x00, 0x00, 0x3A, // kafka records batch size in bytes (container - 8 first offset - 4 batch size)
        // 0x00, 0x00, 0x00, 0x00, //  partitionLeaderEpoch = 0, ignored
        // 0x02, // magic = 2
        // 0x16, 0x3B, 0xB5, 0x6A, // uint crc32
        // 0x00, 0x00, // attributes
        // 0x00, 0x00, 0x00, 0x00, // last offset delta (records.length - 1)
        // 0x00, 0x00, 0x01, 0x5F, 0x8E, 0xBB, 0x3A, 0x0C, // first timestamp (now)
        // 0x00, 0x00, 0x01, 0x5F, 0x8E, 0xBB, 0x3A, 0x0C, // max timestamp
        // 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF // producer id -1
        // 0x00, 0x00, // producer epoch
        // 0x00, 0x00, 0x00, 0x01, // first sequence
        // 0x00, 0x00, 0x00, 0x01, // records length
        // 0x10, // record length zigzag varint
        // 0x00, // record attribute (always 0)
        // 0x00, // varlong timestamp delta
        // 0x00, // var int offset delta
        // 0x02, 0x31,  // var int bytes key
        // 0x02, 0x31,  // var int bytes value
        // 0x00, // var int array header length


        // 0xFF, 0xFF,
        // 0xFF, 0xFF,
        // 0x00, 0x00, 0x03, 0xE8,
        // 0x00, 0x00, 0x00, 0x01,
        // 0x00, 0x04,
        // 0x74, 0x65, 0x73, 0x74,
        // 0x00, 0x00, 0x00, 0x01,
        // 0x00, 0x00, 0x00, 0x00,
        // 0x00, 0x00, 0x00, 0x56, 0x00, 0x00, 0x00, 0x00, 0x02, 0xCD, 0xFC, 0x79, 0xE2, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x21, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x73, 0x74, 0x00, 0x00, 0x00, 0x00



        val produceResponse = connection.sendRequest<ProduceRequestV3, ProduceResponseV1>(produceRequest)
        println(produceResponse)

        connection.close()
    }
}

private fun ByteArray.toHexString() = joinToString(", ", "[", "]") { "0x" + it.toUByte().toString(16).padStart(2, '0').uppercase() }
