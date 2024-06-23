package io.github.vooft.kafka.network.nodejs

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.vooft.kafka.network.nodejs.internal.Socket
import io.github.vooft.kafka.network.nodejs.internal.on
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.io.Buffer
import kotlinx.io.readByteArray
import org.khronos.webgl.Uint8Array
import org.khronos.webgl.get

internal class SocketKafkaMessageAccumulator(
    socket: Socket,
    private val coroutineScope: CoroutineScope
) {
    private val messageBufferChannel = Channel<Buffer>()

    private var currentLength = -1
    private var currentBuffer = Buffer()

    init {
        socket.on(
            onData = { consume(it) },
            onError = { logger.error(it) { "NodeJS socket error" } },
            onClose = { logger.trace { "NodeJS socket closed" } }
        )
    }

    suspend fun receive(): Buffer = messageBufferChannel.receive()

    private fun consume(frame: Uint8Array) {
        logger.trace { "received frame: ${frame.length}" }
        currentBuffer.write(ByteArray(frame.length) { frame[it] })
        updateCurrentLength()

        if (currentLength != -1 && currentBuffer.size >= currentLength) {
            val message = Buffer()
            message.write(currentBuffer.readByteArray(currentLength))

            coroutineScope.launch {
                messageBufferChannel.send(message)
            }

            currentLength = -1
        }

        updateCurrentLength()
    }

    private fun updateCurrentLength() {
        if (currentLength == -1 && currentBuffer.size >= 4) {
            currentLength = currentBuffer.readInt()
        }
    }

    companion object {
        private val logger = KotlinLogging.logger { }
    }
}
