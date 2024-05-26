package io.github.vooft.kafka.common

data class BrokerAddress(val hostname: String, val port: Int) {
    companion object {
        fun fromString(address: String): BrokerAddress {
            val parts = address.split(":")
            require(parts.size == 2) { "Invalid address: $address" }

            return BrokerAddress(parts[0], parts[1].toInt())
        }
    }
}
