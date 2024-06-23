package io.github.vooft.kafka.common.types

import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

@Serializable
@JvmInline
value class NodeId(val value: Int)
