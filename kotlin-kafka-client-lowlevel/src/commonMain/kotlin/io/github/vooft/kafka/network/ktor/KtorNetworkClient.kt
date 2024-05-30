package io.github.vooft.kafka.network.ktor

import io.github.vooft.kafka.network.KafkaConnection
import io.github.vooft.kafka.network.NetworkClient
import io.github.vooft.kafka.network.common.nextHeader
import io.github.vooft.kafka.network.headers.KafkaResponseHeaderV0
import io.github.vooft.kafka.network.messages.KafkaRequest
import io.github.vooft.kafka.network.messages.KafkaResponse
import io.github.vooft.kafka.network.serialization.encodeHeader
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
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
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
    private val writeChannelMutex = Mutex()

    private val readChannel = socket.openReadChannel()
    private val readChannelMutex = Mutex()

    override suspend fun <Rq : KafkaRequest, Rs : KafkaResponse> sendRequest(
        request: Rq,
        requestSerializer: SerializationStrategy<Rq>,
        responseDeserializer: DeserializationStrategy<Rs>
    ): Rs {
        writeChannelMutex.withLock {
            writeChannel.writeMessage {
                val header = request.nextHeader()
                encodeHeader(header)
                encode(requestSerializer, request)
            }
        }

        return readChannelMutex.withLock {
            readChannel.readMessage {
                val header = decode<KafkaResponseHeaderV0>()

                val result = decode(responseDeserializer)

                val remaining = readByteArray()
                require(remaining.isEmpty()) { "Buffer is not empty: ${remaining.toHexString()}" }

                result
            }
        }
    }

    override suspend fun close() {
        socket.close()
    }
}

private suspend fun ByteWriteChannel.writeMessage(block: Sink.() -> Unit) {
    val buffer = Buffer()
    buffer.block()

    val data = buffer.readByteArray()
    writeInt(data.size)
    writeFully(data)

    flush()
}

private suspend fun <T> ByteReadChannel.readMessage(block: Source.() -> T): T {
    val size = readInt()

    val dst = ByteArray(size)
    readFully(dst, 0, size)

    val result = Buffer()
    result.write(dst)
    return result.block()
}

private fun ByteArray.toHexString() = joinToString(", ", "[", "]") { "0x" + it.toUByte().toString(16).padStart(2, '0').uppercase() }
