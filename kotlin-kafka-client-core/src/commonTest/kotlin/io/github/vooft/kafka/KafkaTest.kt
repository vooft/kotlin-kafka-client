package io.github.vooft.kafka

import io.github.vooft.kafka.network.ktor.KtorNetworkClient
import io.github.vooft.kafka.network.messages.ApiVersionsRequestV1
import io.github.vooft.kafka.network.messages.ApiVersionsResponseV1
import io.github.vooft.kafka.network.sendRequest
import kotlinx.coroutines.test.runTest
import kotlin.test.Test

class KafkaTest {
    @Test
    fun test() = runTest {
        val ktorClient = KtorNetworkClient()

        val connection = ktorClient.connect("localhost", 9092)
        val response = connection.sendRequest<ApiVersionsRequestV1, ApiVersionsResponseV1>(ApiVersionsRequestV1)
        println(response)

        connection.close()
    }
}
