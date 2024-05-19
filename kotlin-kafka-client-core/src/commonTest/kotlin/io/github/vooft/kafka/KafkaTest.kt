package io.github.vooft.kafka

import io.github.vooft.kafka.network.dto.ApiVersionRequest
import io.github.vooft.kafka.network.ktor.KtorNetworkClient
import kotlinx.coroutines.test.runTest
import kotlin.test.Test

class KafkaTest {
    @Test
    fun test() = runTest {
        val ktorClient = KtorNetworkClient()

        val connection = ktorClient.connect("localhost", 9092)
        connection.sendRequest(ApiVersionRequest)
        connection.receiveResponse()
    }
}
