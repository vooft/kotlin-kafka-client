package io.github.vooft.kafka.transport.ktor

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.vooft.kafka.transport.KafkaConnection
import io.github.vooft.kafka.transport.KafkaTransport
import io.github.vooft.kafka.transport.dtos.KafkaResponse
import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.Socket
import io.ktor.network.sockets.aSocket
import io.ktor.network.sockets.awaitClosed
import io.ktor.network.sockets.isClosed
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

class KtorKafkaTransport : KafkaTransport {

    private val selectorManager by lazy { SelectorManager(Dispatchers.IO) }

    private val connectionsMutex = Mutex()
    private val connections = mutableListOf<KafkaConnection>()

    override suspend fun connect(host: String, port: Int): KafkaConnection {
        val socket = aSocket(selectorManager).tcp().connect(host, port)
        return KtorKafkaConnection(socket).also {
            connectionsMutex.withLock { connections.add(it) }
        }
    }

    override suspend fun close() {
        connectionsMutex.withLock {
            for (connection in connections) {
                connection.close()
            }
        }
    }
}

private class KtorKafkaConnection(private val socket: Socket) : KafkaConnection {

    private val writeChannel = socket.openWriteChannel()
    private val writeChannelMutex = Mutex()

    private val readChannel = socket.openReadChannel()
    private val readChannelMutex = Mutex()

    override val isClosed: Boolean get() = socket.isClosed

    override suspend fun writeMessage(block: suspend Sink.() -> Unit) {
        writeChannelMutex.withLock {
            writeChannel.writeMessage(block)
        }
    }

    override suspend fun <Rs : KafkaResponse> readMessage(block: suspend Source.() -> Rs): Rs {
        readChannelMutex.withLock {
            return readChannel.readMessage(block)
        }
    }

    override suspend fun close() {
        if (socket.isClosed) {
            return
        }

        socket.close()
        socket.awaitClosed()
    }

    private suspend fun ByteWriteChannel.writeMessage(block: suspend Sink.() -> Unit) {
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

    private suspend fun <T> ByteReadChannel.readMessage(block: suspend Source.() -> T): T {
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




