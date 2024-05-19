package io.github.vooft.kafka.network.ktor

import io.github.vooft.kafka.network.KafkaConnection
import io.github.vooft.kafka.network.NetworkClient
import io.github.vooft.kafka.network.dto.ApiVersion
import io.github.vooft.kafka.network.dto.CorrelationId
import io.github.vooft.kafka.network.dto.KafkaRequest
import io.github.vooft.kafka.network.dto.KafkaRequestHeader
import io.github.vooft.kafka.network.dto.KafkaResponse
import io.github.vooft.kafka.network.write
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
import kotlinx.io.Sink
import kotlinx.io.readByteArray

class KtorNetworkClient : NetworkClient {

    private val selectorManager by lazy { SelectorManager(Dispatchers.IO) }

    override suspend fun connect(host: String, port: Int): KafkaConnection {
        val socket = aSocket(selectorManager).tcp().connect(host, port)
        val writeChannel = socket.openWriteChannel()
        val readChannel = socket.openReadChannel()

        return object : KafkaConnection {
            override suspend fun sendRequest(request: KafkaRequest) {
                val correlationId = CorrelationId.next()
                writeChannel.writeMessage {
                    println("sending message with correlationId: $correlationId")
                    writeHeader(correlationId, request)
                    write(request)
                }
            }

            override suspend fun receiveResponse(): KafkaResponse {
                val buffer = readChannel.readMessage()

                val correlationId = buffer.readInt()
                println("received correlationId: $correlationId")

                val errorCode = buffer.readShort()
                println("received errorCode: $errorCode")

                val length = buffer.readInt()
                println("api versions length: $length")

                repeat(length) {
                    println("api version index $it")

                    val apiKey = buffer.readShort()
                    println("received apiKey: $apiKey")

                    val minVersion = buffer.readShort()
                    println("received minVersion: $minVersion")

                    val maxVersion = buffer.readShort()
                    println("received maxVersion: $maxVersion")

                    println()
                }

                return object : KafkaResponse {}
            }
        }
    }
}

private fun Sink.writeHeader(correlationId: CorrelationId, request: KafkaRequest) = write(
    KafkaRequestHeader(apiKey = request.apiKey, apiVersion = ApiVersion.V1, correlationId = correlationId, clientId = null)
)

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
