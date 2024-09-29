package io.github.vooft.kafka.transport.ktor

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.vooft.kafka.transport.KafkaConnection
import io.github.vooft.kafka.transport.KafkaTransport
import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.Socket
import io.ktor.network.sockets.aSocket
import io.ktor.network.sockets.awaitClosed
import io.ktor.network.sockets.isClosed
import io.ktor.network.sockets.openReadChannel
import io.ktor.network.sockets.openWriteChannel
import io.ktor.utils.io.readByteArray
import io.ktor.utils.io.readInt
import io.ktor.utils.io.writeFully
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.io.Buffer
import kotlinx.io.Source
import kotlinx.io.readByteArray

class KtorKafkaTransport : KafkaTransport {

    private val selectorManager by lazy { SelectorManager() }

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

    override suspend fun writeMessage(source: Source) {
        writeChannelMutex.withLock {
            logger.trace { "Writing message" }
            writeChannel.writeFully(source.readByteArray())
            logger.trace { "Message written, flushing" }
            writeChannel.flush()
            logger.trace { "Flushed" }
        }
    }

    override suspend fun readMessage(): Source {
        readChannelMutex.withLock {
            logger.trace { "Reading message" }

            val size = readChannel.readInt()
            logger.trace { "Reading message with size: $size"}

            val dst = ByteArray(size)
            readChannel.readByteArray(size)

            logger.trace { "Read message with size: $size" }
            return Buffer().apply { write(dst) }
        }
    }

    override suspend fun close() {
        if (socket.isClosed) {
            return
        }

        socket.close()
        socket.awaitClosed()
    }

    companion object {
        private val logger = KotlinLogging.logger {  }
    }
}




