package io.github.vooft.kafka

import io.github.vooft.kafka.network.ktor.KtorNetworkClient
import io.github.vooft.kafka.network.messages.ApiVersionsRequestV0
import io.github.vooft.kafka.network.messages.ApiVersionsResponseV0
import io.github.vooft.kafka.network.sendRequest
import kotlinx.coroutines.test.runTest
import kotlin.test.Test

class KafkaTest {
    @Test
    fun test() = runTest {
        val ktorClient = KtorNetworkClient()

        val connection = ktorClient.connect("localhost", 9092)
        val response = connection.sendRequest<ApiVersionsRequestV0, ApiVersionsResponseV0>(ApiVersionsRequestV0)
        println(response)

        connection.close()
    }
}
