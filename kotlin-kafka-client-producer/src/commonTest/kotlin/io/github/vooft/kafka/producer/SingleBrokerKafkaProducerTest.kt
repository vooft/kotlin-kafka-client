package io.github.vooft.kafka.producer

import io.github.vooft.kafka.network.ktor.KtorNetworkClient
import kotlinx.coroutines.test.runTest
import kotlin.test.Test

class SingleBrokerKafkaProducerTest {
    @Test
    fun test() = runTest {
        val producer = SingleBrokerKafkaProducer(
            connectionProperties = object : KafkaConnectionProperties {
                override val host: String
                    get() = "localhost"
                override val port: Int
                    get() = 9092
            },
            networkClient = KtorNetworkClient()
        )

        producer.send("test", ProducedRecord("my-producer-key", "my-producer-value"))
    }
}
