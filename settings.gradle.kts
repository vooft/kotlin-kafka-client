rootProject.name = "kotlin-kafka-client"

include(":kotlin-kafka-client-core")

include(":client:lowlevel")
include(":client:highlevel")

include(":common:utils")

include(":serialization:serialization-core")

include(":kotlin-kafka-client-transport")

include(":test:benchmark")
include(":test:integration-test")
