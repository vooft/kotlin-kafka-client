package io.github.vooft.kafka.network.ktor

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.vooft.kafka.network.KafkaConnection
import io.github.vooft.kafka.network.KafkaTransport
import io.github.vooft.kafka.network.common.nextHeader
import io.github.vooft.kafka.network.dtos.KafkaRequest
import io.github.vooft.kafka.network.dtos.KafkaResponse
import io.github.vooft.kafka.network.dtos.KafkaResponseHeader
import io.github.vooft.kafka.transport.serialization.decode
import io.github.vooft.kafka.transport.serialization.encode
import io.github.vooft.kafka.utils.toHexString
import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.Socket
import io.ktor.network.sockets.aSocket
import io.ktor.network.sockets.awaitClosed
import io.ktor.network.sockets.isClosed
import io.ktor.network.sockets.openReadChannel
import io.ktor.network.sockets.openWriteChannel
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.ByteWriteChannel
import io.ktor.utils.io.CancellationException
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

class KtorKafkaTransport : KafkaTransport {

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

    override val isClosed: Boolean get() = socket.isClosed

    override suspend fun <Rq : KafkaRequest, Rs : KafkaResponse> sendRequest(
        request: Rq,
        requestSerializer: SerializationStrategy<Rq>,
        responseDeserializer: DeserializationStrategy<Rs>
    ): Rs {
        // TODO: add auto-closing after a timeout
        require(!socket.isClosed) { "Socket is closed" }

        try {
            writeChannel.writeMessage {
                val header = request.nextHeader()

                encode(header)
                encode(requestSerializer, request)
            }

            return readChannel.readMessage {
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

    override suspend fun close() {
        if (socket.isClosed) {
            return
        }

        socket.close()
        socket.awaitClosed()
    }

    private suspend fun ByteWriteChannel.writeMessage(block: Sink.() -> Unit) = writeChannelMutex.withLock {
        logger.trace { "Writing message" }

        val buffer = Buffer()
        buffer.block()

        val data = buffer.readByteArray()

        logger.trace { "Writing message size ${data.size}" }
        writeInt(data.size)

        logger.trace { "Writing message itself" }
        writeFully(data)

        logger.trace { "Message written, flushing" }
        flush()
        logger.trace { "Flushed" }
    }

    private suspend fun <T> ByteReadChannel.readMessage(block: Source.() -> T): T = readChannelMutex.withLock {
        logger.trace { "Reading message" }

        val size = readInt()
        logger.trace { "Reading message with size: $size"}

        val dst = ByteArray(size)
        readFully(dst, 0, size)

        logger.trace { "Read message with size: $size" }

        val result = Buffer()
        result.write(dst)
        return result.block()
    }

    companion object {
        private val logger = KotlinLogging.logger {  }
    }
}




