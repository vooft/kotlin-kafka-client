package io.github.vooft.kafka

import io.github.vooft.kafka.serialization.KafkaSerde
import io.github.vooft.kafka.serialization.common.primitives.Crc32cPrefixed
import io.github.vooft.kafka.serialization.common.primitives.Int32BytesSizePrefixed
import io.github.vooft.kafka.serialization.common.primitives.Int32List
import io.github.vooft.kafka.serialization.common.primitives.VarInt
import io.github.vooft.kafka.serialization.common.primitives.VarIntByteArray
import io.github.vooft.kafka.serialization.common.primitives.VarIntBytesSizePrefixed
import io.github.vooft.kafka.serialization.common.primitives.VarLong
import io.github.vooft.kafka.serialization.common.primitives.toInt32List
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex
import io.github.vooft.kafka.serialization.common.wrappers.PartitionOffset
import io.github.vooft.kafka.serialization.decode
import io.github.vooft.kafka.serialization.encode
import io.github.vooft.kafka.transport.common.ErrorCode
import io.github.vooft.kafka.transport.messages.FetchRequestV4
import io.github.vooft.kafka.transport.messages.FetchResponseV4
import io.github.vooft.kafka.transport.messages.KafkaRecordBatchContainerV0
import io.github.vooft.kafka.transport.messages.KafkaRecordV0
import kotlinx.benchmark.Benchmark
import kotlinx.benchmark.BenchmarkMode
import kotlinx.benchmark.BenchmarkTimeUnit
import kotlinx.benchmark.Measurement
import kotlinx.benchmark.Mode
import kotlinx.benchmark.OutputTimeUnit
import kotlinx.benchmark.Scope
import kotlinx.benchmark.Setup
import kotlinx.benchmark.State
import kotlinx.benchmark.Warmup
import kotlinx.io.Buffer

@Suppress("unused")
@State(Scope.Benchmark)
@Measurement(iterations = 5, time = 7, timeUnit = BenchmarkTimeUnit.SECONDS)
@Warmup(iterations = 5, time = 5, timeUnit = BenchmarkTimeUnit.SECONDS)
@OutputTimeUnit(BenchmarkTimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.Throughput)
class MultiplatformKafkaBenchmark {

    private lateinit var fetchRequest: FetchRequestV4
    private lateinit var encodedFetchRequest: Buffer

    private lateinit var fetchResponse: FetchResponseV4
    private lateinit var encodedFetchResponse: Buffer

    @Suppress("detekt:LongMethod")
    @Setup
    fun setUp() {
        fetchRequest = FetchRequestV4(
            maxWaitTime = 1000,
            minBytes = 2048,
            maxBytes = 10240,
            topics = (1..5).map { topic ->
                FetchRequestV4.Topic(
                    topic = KafkaTopic("topic$topic"),
                    partitions = (1..5).map {
                        FetchRequestV4.Topic.Partition(
                            partition = PartitionIndex(it),
                            fetchOffset = PartitionOffset(0),
                            maxBytes = 10240
                        )
                    }.toInt32List()
                )
            }.toInt32List()
        )

        encodedFetchRequest = KafkaSerde.encode(fetchRequest)

        fetchResponse = FetchResponseV4(
            throttleTimeMs = 0,
            topics = (1..5).map { topic ->
                FetchResponseV4.Topic(
                    topic = KafkaTopic("topic$topic"),
                    partitions = (1..5).map { partition ->
                        FetchResponseV4.Topic.Partition(
                            partition = PartitionIndex(partition),
                            errorCode = ErrorCode.NO_ERROR,
                            highwaterMarkOffset = PartitionOffset(0),
                            lastStableOffset = PartitionOffset(0),
                            abortedTransactions = Int32List(),
                            batchContainer = Int32BytesSizePrefixed(
                                KafkaRecordBatchContainerV0(
                                    batch = Int32BytesSizePrefixed(
                                        KafkaRecordBatchContainerV0.KafkaRecordBatch(
                                            partitionLeaderEpoch = 0,
                                            magic = 2,
                                            body = Crc32cPrefixed(
                                                KafkaRecordBatchContainerV0.KafkaRecordBatch.KafkaRecordBatchBody(
                                                    attributes = 0,
                                                    lastOffsetDelta = 0,
                                                    firstTimestamp = 0,
                                                    maxTimestamp = 0,
                                                    producerId = -1,
                                                    producerEpoch = -1,
                                                    records = (1..10).map {
                                                        KafkaRecordV0(
                                                            recordBody = VarIntBytesSizePrefixed(
                                                                KafkaRecordV0.KafkaRecordBody(
                                                                    attributes = 0,
                                                                    timestampDelta = VarLong.fromDecoded(100),
                                                                    offsetDelta = VarInt.fromDecoded(1000),
                                                                    recordKey = VarIntByteArray("key-$it".encodeToByteArray()),
                                                                    recordValue = VarIntByteArray("value-$it".encodeToByteArray())
                                                                )
                                                            )
                                                        )
                                                    }.toInt32List()

                                                )
                                            )
                                        )
                                    )
                                )
                            ),
                        )
                    }.toInt32List()
                )
            }.toInt32List()
        )

        encodedFetchResponse = KafkaSerde.encode(fetchResponse)
    }

    @Benchmark
    fun requestEncode(): Buffer {
        return KafkaSerde.encode(fetchRequest)
    }

    @Benchmark
    fun requestDecode(): Any {
        return KafkaSerde.decode<FetchRequestV4>(encodedFetchRequest.peek())
    }

    @Benchmark
    fun responseEncode(): Buffer {
        return KafkaSerde.encode(fetchResponse)
    }

    @Benchmark
    fun responseDecode(): Any {
        return KafkaSerde.decode<FetchResponseV4>(encodedFetchResponse.peek())
    }
}
