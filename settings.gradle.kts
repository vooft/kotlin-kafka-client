rootProject.name = "kotlin-kafka-client"

include(":kotlin-kafka-client-benchmark")
include(":kotlin-kafka-client-lowlevel")
include(":kotlin-kafka-client-core")
include(":kotlin-kafka-client-test")
include(":kotlin-kafka-client-common")
include(":kotlinx-serialization-kafka-common")

include(":serialization:kafka-serde")

include(":transport:transport-core")
include(":transport:transport-ktor")
include(":transport:transport-factory")
include(":transport:transport-serialization")
