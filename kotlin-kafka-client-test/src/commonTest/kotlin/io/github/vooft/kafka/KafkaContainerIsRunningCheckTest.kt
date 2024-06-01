package io.github.vooft.kafka

import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.aSocket
import io.ktor.utils.io.core.use
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.test.runTest
import kotlin.test.Test

class KafkaContainerIsRunningCheckTest {
    @Test
    fun `please run make start-kafka from the project root if this test fails`() = runTest {
        try {
            SelectorManager(Dispatchers.IO).use { selectorManager ->
                aSocket(selectorManager).tcp().connect("localhost", 9093)
            }
        } catch (e: Exception) {
            throw AssertionError("Failed to connect to Kafka container. Please run `make start-kafka` from the project root.", e)
        }
    }
}
