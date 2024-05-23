package io.github.vooft.kafka.producer

import io.github.vooft.kafka.network.KafkaConnection
import io.github.vooft.kafka.network.NetworkClient
import io.github.vooft.kafka.network.messages.ProduceRequestV3
import io.github.vooft.kafka.network.messages.ProduceResponseV3
import io.github.vooft.kafka.network.sendRequest
import io.github.vooft.kafka.producer.requests.ProduceRequestFactory
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Job
import kotlinx.coroutines.async

class SingleBrokerKafkaProducer(
    private val connectionProperties: KafkaConnectionProperties,
    private val networkClient: NetworkClient,
    private val coroutineScope: CoroutineScope = CoroutineScope(Job()),
) : KafkaProducer {

    private val connectionDeferred: Deferred<KafkaConnection> = coroutineScope.async(start = CoroutineStart.LAZY) {
        networkClient.connect(connectionProperties.host, connectionProperties.port)
    }

    override suspend fun send(topic: String, record: ProducedRecord): RecordMetadata {
        val connection = connectionDeferred.await()

        val request = ProduceRequestFactory.createProduceRequest(topic, listOf(record))
        val response = connection.sendRequest<ProduceRequestV3, ProduceResponseV3>(request)

        return RecordMetadata(
            topic = response.topicResponses.single().topicName.nonNullValue,
            partition = response.topicResponses.single().partitionResponses.single().index,
            errorCode = response.topicResponses.single().partitionResponses.single().errorCode
        )
    }
}
