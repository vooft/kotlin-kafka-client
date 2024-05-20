package io.github.vooft.kafka

import io.github.vooft.kafka.network.ktor.KtorNetworkClient
import io.github.vooft.kafka.network.messages.ApiVersionsRequestV1
import io.github.vooft.kafka.network.messages.ApiVersionsResponseV1
import io.github.vooft.kafka.network.messages.MetadataRequestV1
import io.github.vooft.kafka.network.messages.MetadataResponseV1
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
        val versionsResponse = connection.sendRequest<ApiVersionsRequestV1, ApiVersionsResponseV1>(ApiVersionsRequestV1)
        println(versionsResponse)

        val metadataResponse = connection.sendRequest<MetadataRequestV1, MetadataResponseV1>(MetadataRequestV1(listOf("test")))
        println(metadataResponse)

        connection.close()
    }
}

private fun ByteArray.toHexString() = joinToString(", ", "[", "]") { "0x" + it.toUByte().toString(16).padStart(2, '0').uppercase() }
