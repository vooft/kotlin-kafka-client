rootProject.name = "kotlin-kafka-client"

include(":kotlin-kafka-client-core")

include(":client:lowlevel")
include(":client:highlevel")

include(":common:utils")

include(":serialization:serialization-core")
include(":serialization:serialization-types")

include(":transport:transport-core")
include(":transport:transport-ktor")
include(":transport:transport-factory")
include(":transport:transport-serialization")

include(":test:benchmark")
include(":test:integration-test")
