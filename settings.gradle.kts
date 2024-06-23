rootProject.name = "kotlin-kafka-client"

include(":kotlin-kafka-client-core")

include(":client:lowlevel")
include(":client:highlevel")

include(":common:utils")

include(":serialization:serialization-core")

include(":transport:transport-core")
include(":transport:transport-ktor")
include(":transport:transport-nodejs")
include(":transport:transport-factory")

include(":test:benchmark")
include(":test:integration-test")
