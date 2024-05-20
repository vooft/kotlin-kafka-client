package io.github.vooft.kafka.network.ktor

import io.github.vooft.kafka.network.KafkaConnection
import io.github.vooft.kafka.network.NetworkClient
import io.github.vooft.kafka.network.messages.ApiVersionsResponseV0
import io.github.vooft.kafka.network.messages.KafkaRequest
import io.github.vooft.kafka.network.messages.KafkaRequestHeader
import io.github.vooft.kafka.network.messages.KafkaResponse
import io.github.vooft.kafka.network.messages.KafkaResponseHeader
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
import kotlinx.io.readByteArray

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

    override suspend fun sendRequest(request: KafkaRequest) {
        val correlationId = correlationIdCounter++
        writeChannel.writeMessage {
            println("sending message with correlationId: $correlationId")
            encode(request.createHeader())
            encode(request)
        }
    }

    override suspend fun receiveResponse(): KafkaResponse {
        val buffer = readChannel.readMessage()

        val header = buffer.decode<KafkaResponseHeader>()
        println("received header: $header")

        val response = buffer.decode<ApiVersionsResponseV0>()
        println("received response: $response")

        val remaining = buffer.readByteArray()
        require(remaining.isEmpty()) { "Buffer is not empty: ${remaining.toHexString()}" }

        return response
    }

    override suspend fun close() {
        socket.close()
    }

    private fun KafkaRequest.createHeader(): KafkaRequestHeader {
        return KafkaRequestHeader(
            apiKey = apiKey,
            apiVersion = version,
            correlationId = correlationIdCounter++,
            clientId = null
        )
    }
}

private suspend fun ByteWriteChannel.writeMessage(block: Buffer.() -> Unit) {
    val buffer = Buffer()
    buffer.block()

    val data = buffer.readByteArray()
    writeInt(data.size)
    writeFully(data)

    flush()
}

private suspend fun ByteReadChannel.readMessage(): Buffer {
    val size = readInt()
    println("Reading message of size $size")

    val dst = ByteArray(size)
    readFully(dst, 0, size)

    println("Read message: ${dst.toHexString()}")

    val result = Buffer()
    result.write(dst)
    return result
}

private fun ByteArray.toHexString() = joinToString(", ", "[", "]") { "0x" + it.toUByte().toString(16).padStart(2, '0').uppercase() }
