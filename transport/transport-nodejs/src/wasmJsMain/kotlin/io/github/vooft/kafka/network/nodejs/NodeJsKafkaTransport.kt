package io.github.vooft.kafka.network.nodejs

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.vooft.kafka.network.KafkaConnection
import io.github.vooft.kafka.network.KafkaTransport
import io.github.vooft.kafka.network.dtos.KafkaResponse
import io.github.vooft.kafka.network.nodejs.internal.Socket
import io.github.vooft.kafka.network.nodejs.internal.connect
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.io.Buffer
import kotlinx.io.Sink
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

    override suspend fun writeMessage(block: suspend Sink.() -> Unit) {
        writeMutex.withLock {
            val data = Buffer().apply { block() }.readByteArray()

            val lengthBuffer = Buffer()
            lengthBuffer.writeInt(data.size)

            val lengthWrite = socket.write(Uint8Array(lengthBuffer.readByteArray().toJsArray()))
            logger.trace { "written length $lengthWrite" }

            val result = socket.write(Uint8Array(data.toJsArray()))
            logger.trace { "write result $result" }
        }
    }

    override suspend fun <Rs : KafkaResponse> readMessage(block: suspend Source.() -> Rs): Rs {
        readMutex.withLock {
            val buffer = accumulator.receive()
            return buffer.block()
        }
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
