package io.github.vooft.kafka.transport.nodejs

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.vooft.kafka.transport.KafkaConnection
import io.github.vooft.kafka.transport.KafkaTransport
import io.github.vooft.kafka.transport.nodejs.internal.Socket
import io.github.vooft.kafka.transport.nodejs.internal.connect
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.io.Source
import kotlinx.io.readByteArray
import org.khronos.webgl.Uint8Array

class NodeJsKafkaTransport(private val coroutineScope: CoroutineScope) : KafkaTransport {

    private val connectionsMutex = Mutex()
    private val connections = mutableListOf<Socket>()

    override suspend fun connect(host: String, port: Int): KafkaConnection {
        val socket = connect(port, host)
        connectionsMutex.withLock { connections.add(socket) }
        return NodeJsKafkaConnection(socket, coroutineScope)
    }

    override suspend fun close() {
        for (connection in connections) {
            connection.destroy()
        }
    }
}

private class NodeJsKafkaConnection(private val socket: Socket, coroutineScope: CoroutineScope) : KafkaConnection {

    private val readMutex = Mutex()
    private val writeMutex = Mutex()
    private val accumulator = SocketKafkaMessageAccumulator(socket, coroutineScope)

    override val isClosed: Boolean
        get() = socket.readyState != "open" && socket.readyState != "opening"

    override suspend fun writeMessage(source: Source) {
        writeMutex.withLock {
            require(socket.write(Uint8Array(source.readByteArray().toJsArray()))) {
                "Failed to write message"
            }
        }
    }

    override suspend fun readMessage(): Source {
        readMutex.withLock { return accumulator.receive() }
    }

    override suspend fun close() {
        socket.destroy()
    }

    companion object {
        private val logger = KotlinLogging.logger { }
    }
}

private fun ByteArray.toJsArray(): JsArray<JsNumber> {
    val result = JsArray<JsNumber>()
    forEachIndexed { index, byte -> result[index] = byte.toInt().toJsNumber() }
    return result
}
