package io.github.vooft.kafka.transport

import io.github.vooft.kafka.transport.common.nextHeader
import io.github.vooft.kafka.transport.dtos.KafkaRequest
import io.github.vooft.kafka.transport.dtos.KafkaResponse
import io.github.vooft.kafka.transport.dtos.KafkaResponseHeader
import io.github.vooft.kafka.transport.serialization.decode
import io.github.vooft.kafka.transport.serialization.encode
import io.github.vooft.kafka.utils.toHexString
import kotlinx.io.Buffer
import kotlinx.io.readByteArray
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.serializer
import kotlin.coroutines.cancellation.CancellationException

suspend inline fun <reified Rq : KafkaRequest, reified Rs : KafkaResponse> KafkaConnection.sendRequest(request: Rq): Rs =
    sendRequest(request, serializer<Rq>(), serializer<Rs>())

suspend fun <Rq : KafkaRequest, Rs : KafkaResponse> KafkaConnection.sendRequest(
    request: Rq,
    requestSerializer: SerializationStrategy<Rq>,
    responseDeserializer: DeserializationStrategy<Rs>
): Rs {
    require(!isClosed) { "Socket is closed" }

    try {
        val requestBuf = Buffer().apply {
            val header = request.nextHeader()

            encode(header)
            encode(requestSerializer, request)
        }

        writeMessage(requestBuf.withSize())

        return readMessage().run {
            val header = decode<KafkaResponseHeader>()
            // TODO: use version from the header to determine which one to deserialize

            val result = decode(responseDeserializer)

            val remaining = readByteArray()
            require(remaining.isEmpty()) { "Buffer is not empty: ${remaining.toHexString()}" }

            result
        }
    } catch (e: CancellationException) {
        close()
        throw e
    }
}

private fun Buffer.withSize() = Buffer().also {
    it.writeInt(size.toInt())
    it.write(this, size)
}
