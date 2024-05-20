package io.github.vooft.kafka.network.ktor

import io.github.vooft.kafka.network.KafkaConnection
import io.github.vooft.kafka.network.NetworkClient
import io.github.vooft.kafka.network.messages.ApiVersionsRequestV0.apiKey
import io.github.vooft.kafka.network.messages.CorrelationId
import io.github.vooft.kafka.network.messages.KafkaRequest
import io.github.vooft.kafka.network.messages.KafkaRequestHeaderV0
import io.github.vooft.kafka.network.messages.KafkaResponse
import io.github.vooft.kafka.network.messages.KafkaResponseHeader
import io.github.vooft.kafka.network.messages.KafkaResponseHeaderV0
import io.github.vooft.kafka.network.messages.VersionedV0
import io.github.vooft.kafka.serialization.decode
import io.github.vooft.kafka.serialization.encode
import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.Socket
import io.ktor.network.sockets.aSocket
import io.ktor.network.sockets.openReadChannel
import io.ktor.network.sockets.openWriteChannel
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.ByteWriteChannel
import io.ktor.utils.io.writeFully
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.io.Buffer
import kotlinx.io.Sink
import kotlinx.io.Source
import kotlinx.io.readByteArray
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy

class KtorNetworkClient : NetworkClient {

    private val selectorManager by lazy { SelectorManager(Dispatchers.IO) }

    override suspend fun connect(host: String, port: Int): KafkaConnection {
        val socket = aSocket(selectorManager).tcp().connect(host, port)
        return KtorKafkaConnection(socket)
    }
}

private class KtorKafkaConnection(private val socket: Socket) : KafkaConnection {

    private val writeChannel = socket.openWriteChannel()
    private val readChannel = socket.openReadChannel()

    private var correlationIdCounter = 123 // TODO: replace with Atomic

    override suspend fun <Rq : KafkaRequest, Rs : KafkaResponse> sendRequest(
        request: Rq,
        requestSerializer: SerializationStrategy<Rq>,
        responseDeserializer: DeserializationStrategy<Rs>
    ): Rs {
        writeChannel.writeMessage {
            encodeHeaderFor(request)
            encode(requestSerializer, request)
        }

        return readChannel.readMessage {
            decode(request.responseHeaderDeserializer())

            val result = decode(responseDeserializer)

            val remaining = readByteArray()
            require(remaining.isEmpty()) { "Buffer is not empty: ${remaining.toHexString()}" }

            result
        }
    }

    override suspend fun close() {
        socket.close()
    }

    private fun Sink.encodeHeaderFor(request: KafkaRequest) {
        val correlationId = CorrelationId.next()
        println("sending message with correlation id $correlationId")
        when (request) {
            is VersionedV0 -> encode(
                KafkaRequestHeaderV0(
                    apiKey = apiKey,
                    correlationId = correlationId
                )
            )
        }
    }

    private fun KafkaRequest.responseHeaderDeserializer(): DeserializationStrategy<KafkaResponseHeader> = when (this) {
        is VersionedV0 -> KafkaResponseHeaderV0.serializer()
    }
}

private suspend fun ByteWriteChannel.writeMessage(block: Sink.() -> Unit) {
    val buffer = Buffer()
    buffer.block()

    val data = buffer.readByteArray()
    println("writing ${data.size} bytes")
    writeInt(data.size)
    writeFully(data)

    flush()
    println("sent message: ${data.toHexString()}")
}

private suspend fun <T> ByteReadChannel.readMessage(block: Source.() -> T): T {
    println("reading message")
    val size = readInt()
    println("Reading message of size $size")

    val dst = ByteArray(size)
    readFully(dst, 0, size)

    println("Read message: ${dst.toHexString()}")

    val result = Buffer()
    result.write(dst)
    return result.block()
}

private fun ByteArray.toHexString() = joinToString(", ", "[", "]") { "0x" + it.toUByte().toString(16).padStart(2, '0').uppercase() }
