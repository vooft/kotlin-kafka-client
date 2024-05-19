import org.apache.kafka.common.message.ApiVersionsRequestData
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.ApiVersionsRequest
import org.apache.kafka.common.requests.RequestHeader

fun main() {
    val request = ApiVersionsRequest(ApiVersionsRequestData(), 2.toShort())

    val buf = request.serializeWithHeader(RequestHeader(
        ApiKeys.API_VERSIONS, 2.toShort(), null, 666
    ))
    println(buf.remaining())
    val arr = ByteArray(buf.remaining())
    buf.get(arr)

    println("boo")
    println(arr.toHexString())

    // [
    // 0x00, 0x12, // short 18 apiKey
    // 0x00, 0x02, // short version 2
    // 0x00, 0x00, 0x02, 0x9A, // int correlationId 666
    // 0xFF, 0xFF // short -1 clientId
    // ]

    Thread.sleep(Long.MAX_VALUE)
}

private fun ByteArray.toHexString() = joinToString(", ", "[", "]") { "0x" + it.toUByte().toString(16).padStart(2, '0').uppercase() }


