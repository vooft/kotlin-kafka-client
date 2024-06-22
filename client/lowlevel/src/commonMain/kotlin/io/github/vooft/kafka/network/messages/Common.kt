package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.network.dtos.ApiVersion
import io.github.vooft.kafka.network.dtos.Versioned

sealed interface VersionedV0 : Versioned {
    override val apiVersion: ApiVersion get() = ApiVersion.V0
}

sealed interface VersionedV1 : Versioned {
    override val apiVersion: ApiVersion get() = ApiVersion.V1
}

sealed interface VersionedV3: Versioned {
    override val apiVersion: ApiVersion get() = ApiVersion.V3
}

sealed interface VersionedV4 : Versioned {
    override val apiVersion: ApiVersion get() = ApiVersion.V4
}
