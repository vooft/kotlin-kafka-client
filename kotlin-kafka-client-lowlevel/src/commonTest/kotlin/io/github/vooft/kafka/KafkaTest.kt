package io.github.vooft.kafka

import io.github.vooft.kafka.network.common.toInt16String
import io.github.vooft.kafka.network.ktor.KtorNetworkClient
import io.github.vooft.kafka.network.messages.CoordinatorType.GROUP
import io.github.vooft.kafka.network.messages.FindCoordinatorRequestV1
import io.github.vooft.kafka.network.messages.FindCoordinatorResponseV1
import io.github.vooft.kafka.network.messages.JoinGroupRequestV1
import io.github.vooft.kafka.network.messages.JoinGroupResponseV1
import io.github.vooft.kafka.network.sendRequest
import io.github.vooft.kafka.serialization.common.primitives.Int32BytesSizePrefixed
import io.github.vooft.kafka.serialization.common.primitives.int32ListOf
import io.github.vooft.kafka.serialization.common.wrappers.GroupId
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.MemberId
import kotlinx.coroutines.test.runTest

class KafkaTest {
//    @Test
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
                        metadata = Int32BytesSizePrefixed(
                            JoinGroupRequestV1.GroupProtocol.Metadata(
                                topics = int32ListOf(KafkaTopic("test"))
                            )
                        )
                    )
                )
            )
        )

        println(joinGroupResponse)


        connection.close()
    }
}
