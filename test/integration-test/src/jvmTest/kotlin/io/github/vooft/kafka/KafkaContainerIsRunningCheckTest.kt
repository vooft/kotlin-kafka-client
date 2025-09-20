package io.github.vooft.kafka

import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.aSocket
import kotlinx.coroutines.runBlocking
import kotlin.test.Test

class KafkaContainerIsRunningCheckTest {
    @Test
    fun `please run make start-kafka from the project root if this test fails`(): Unit = runBlocking {
        try {
            SelectorManager().use { selectorManager ->
                aSocket(selectorManager).tcp().connect("localhost", 9093)
            }
        } catch (e: Exception) {
            throw AssertionError("Failed to connect to Kafka container. Please run `make start-kafka` from the project root.", e)
        }
    }
}
