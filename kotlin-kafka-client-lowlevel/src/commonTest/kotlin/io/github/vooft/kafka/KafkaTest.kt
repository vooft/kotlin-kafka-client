package io.github.vooft.kafka

import io.github.vooft.kafka.common.GroupId
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
import io.github.vooft.kafka.serialization.common.primitives.int32ListOf
import kotlinx.coroutines.test.runTest
import kotlin.test.Test

class KafkaTest {
    @Test
    fun test() = runTest {
        val ktorClient = KtorNetworkClient()

        val connection = ktorClient.connect("localhost", 9093)

        val group = GroupId("test-group")
        val findCoordinatorResponse = connection.sendRequest<FindCoordinatorRequestV1, FindCoordinatorResponseV1>(
            FindCoordinatorRequestV1(key = group.value.toInt16String(), keyType = GROUP)
        )
        println(findCoordinatorResponse)

        val joinGroupResponse = connection.sendRequest<JoinGroupRequestV1, JoinGroupResponseV1>(
            JoinGroupRequestV1(
                groupId = group,
                sessionTimeoutMs = 30000,
                rebalanceTimeoutMs = 10000,
                memberId = MemberId(""),
                protocolType = "consumer".toInt16String(),
                groupProtocols = int32ListOf(
                    JoinGroupRequestV1.GroupProtocol(
                        protocol = "mybla".toInt16String(),
                        metadata = JoinGroupRequestV1.GroupProtocol.Metadata(
                            topics = int32ListOf(KafkaTopic("test"))
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
