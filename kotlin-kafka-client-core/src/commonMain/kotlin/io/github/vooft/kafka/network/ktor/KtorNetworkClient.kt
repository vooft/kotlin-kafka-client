package io.github.vooft.kafka.network.ktor

import io.github.vooft.kafka.network.KafkaConnection
import io.github.vooft.kafka.network.NetworkClient
import io.github.vooft.kafka.network.dto.ApiVersion
import io.github.vooft.kafka.network.dto.ApiVersionsResponse
import io.github.vooft.kafka.network.dto.KafkaRequest
import io.github.vooft.kafka.network.dto.KafkaRequestHeader
import io.github.vooft.kafka.network.dto.KafkaResponse
import io.github.vooft.kafka.network.dto.KafkaResponseHeader
import io.github.vooft.kafka.serialization.decode
import io.github.vooft.kafka.serialization.encode
import io.ktor.network.selector.SelectorManager
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
        val writeChannel = socket.openWriteChannel()
        val readChannel = socket.openReadChannel()

        return object : KafkaConnection {
            private var correlationIdCounter = 123

            override suspend fun sendRequest(request: KafkaRequest) {
                val correlationId = correlationIdCounter++
                writeChannel.writeMessage {
                    println("sending message with correlationId: $correlationId")
                    encode(
                        KafkaRequestHeader(
                            apiKey = request.apiKey,
                            apiVersion = ApiVersion.V1,
                            correlationId = correlationId,
                            clientId = null
                        )
                    )
                    encode(request)
                }
            }

            override suspend fun receiveResponse(): KafkaResponse {
                val buffer = readChannel.readMessage()

                val header = buffer.decode<KafkaResponseHeader>()
                println("received header: $header")

                val response = buffer.decode<ApiVersionsResponse>()
                println("received response: $response")

                println("remaining: ${buffer.readByteArray().toHexString()}")

                return object : KafkaResponse {}
            }
        }
    }
}

private suspend fun ByteWriteChannel.writeMessage(block: Buffer.() -> Unit) {
    val buffer = Buffer()
    buffer.block()

    val array = buffer.readByteArray()
    writeMessage(array)
}

private suspend fun ByteWriteChannel.writeMessage(data: ByteArray) {
    println("Writing message of size ${data.size}: ${data.toHexString()}")
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
