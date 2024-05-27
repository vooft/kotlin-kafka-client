package io.github.vooft.kafka

import io.github.vooft.kafka.common.KafkaTopic
import io.github.vooft.kafka.common.MemberId
import io.github.vooft.kafka.network.common.toInt16String
import io.github.vooft.kafka.network.ktor.KtorNetworkClient
import io.github.vooft.kafka.network.messages.CoordinatorType.GROUP
import io.github.vooft.kafka.network.messages.FindCoordinatorRequestV1
import io.github.vooft.kafka.network.messages.FindCoordinatorResponseV1
import io.github.vooft.kafka.network.messages.JoinGroupRequestV1
import io.github.vooft.kafka.network.messages.JoinGroupResponseV1
import io.github.vooft.kafka.network.sendRequest
import kotlinx.coroutines.test.runTest
import kotlinx.io.Buffer
import kotlinx.io.readByteArray
import kotlin.test.Test

class KafkaTest {
    @Test
    fun test() = runTest {
        val ktorClient = KtorNetworkClient()

        Buffer().apply {
            writeInt(65540)
            println(readByteArray().toHexString())
        }

        val connection = ktorClient.connect("localhost", 9093)

        val group = "test-group".toInt16String()
        val findCoordinatorResponse = connection.sendRequest<FindCoordinatorRequestV1, FindCoordinatorResponseV1>(
            FindCoordinatorRequestV1(key = group, keyType = GROUP)
        )
        println(findCoordinatorResponse)

        val joinGroupResponse = connection.sendRequest<JoinGroupRequestV1, JoinGroupResponseV1>(
            JoinGroupRequestV1(
                groupId = group,
                sessionTimeoutMs = 30000,
                rebalanceTimeoutMs = 10000,
                memberId = MemberId(""),
                protocolType = "consumer".toInt16String(),
                groupProtocols = listOf(
                    JoinGroupRequestV1.GroupProtocol(
                        protocol = "mybla".toInt16String(),
                        metadata = JoinGroupRequestV1.GroupProtocol.Metadata(
                            topics = listOf(KafkaTopic("test"))
                        )
                    )
                )
            )
        )

        println(joinGroupResponse)


        connection.close()
    }
}

private fun ByteArray.toHexString() = joinToString(", ", "[", "]") { "0x" + it.toUByte().toString(16).padStart(2, '0').uppercase() }
