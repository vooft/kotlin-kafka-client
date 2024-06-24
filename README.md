![Build and test](https://github.com/vooft/kotlin-kafka-client/actions/workflows/build.yml/badge.svg?branch=main)
![Releases](https://img.shields.io/github/v/release/vooft/kotlin-kafka-client)
![Maven Central](https://img.shields.io/maven-central/v/io.github.vooft/kotlin-kafka-client-core)
![License](https://img.shields.io/github/license/vooft/kotlin-kafka-client)

![badge-platform-jvm]
![badge-platform-js-node]
![badge-platform-wasm]
![badge-platform-linux]
![badge-platform-macos]
![badge-platform-ios]
![badge-support-apple-silicon]

# kotlin-kafka-client
Kotlin Multiplatform implementation of Kafka client, written from scratch.

This library is at the moment in early development stage and should not be used in production.

Used technologies:
* Kotlin Coroutines
* Network layer
    * `net.socket` from NodeJS wrapper for JS and WASM
    * `ktor-network` for other platforms
* Serialization - `kotlinx-serialization` (with custom serializer for Kafka protocol)

## Demo app

There is a demo app built using Compose Multiplatform that runs on Android, iOS and Desktop: [kotlin-kafka-client-demo](https://github.com/vooft/kotlin-kafka-client-demo)

# Quick start
Library is published to Maven Central under name [io.github.vooft:kotlin-kafka-client-core](https://central.sonatype.com/search?namespace=io.github.vooft&name=kotlin-kafka-client-core).

Add the dependency to your project:
```kotlin
kotlin {
    ...

    sourceSets {
        commonMain.dependencies {
            implementation("io.github.vooft:kotlin-kafka-client-core:<version>")
        }
    }
}
```

Then in your code, start with class `KafkaCluster`:

```kotlin
fun main() {
    // define bootstrap servers
    val bootstrapServers = listOf(
      BrokerAddress("localhost", 9092),
      BrokerAddress("localhost", 9093),
      BrokerAddress("localhost", 9094)
    )
    
    // create instance of KafkaCluster
    val cluster = KafkaCluster(bootstrapServers)
    
    // both producer and consumer are bound to a single topic
    
    val producer = cluster.createProducer(KafkaTopic("my-topic"))
    producer.send("my-key", "my-value")
    
    // consumer can be used both with group and without (in this case group is not created and no offset remembered)
    val consumer = cluster.createConsumer(KafkaTopic("my-topic"), GroupId("my-group"))
    
    // low-level API is similar to the kafka-clients implementation, but Flow-based API is on the way
    consumer.consume().forEach {
      println(it)
    }
}
```

# Goals and non-goals

Main goal for this library is to provide a simple library to use for typical Kafka use-cases, 
such as produce to a topic from one service and consume from it in another, optionally using groups to consume in parallel.

It is not a goal to implement all the features of Kafka protocol, for example to be used in Kafka Streams.

# Supported features

At the moment library supports only basic features of Kafka protocol:
* Cluster
  * Multiple bootstrap servers
  * Multiple brokers
* Producer
  * Produce records to a topic with multiple partitions on different brokers
  * Batching records publishing to different servers
  * Using key to determine partition
* Consumer
  * Consume records from a topic with multiple partitions on different brokers
  * Using group to consume records in parallel
  * Heartbeat to maintain group membership

<!-- TAG_PLATFORMS -->
[badge-platform-android]: http://img.shields.io/badge/-android-6EDB8D.svg?style=flat
[badge-platform-jvm]: http://img.shields.io/badge/-jvm-DB413D.svg?style=flat
[badge-platform-js]: http://img.shields.io/badge/-js-F8DB5D.svg?style=flat
[badge-platform-js-node]: https://img.shields.io/badge/-nodejs-68a063.svg?style=flat
[badge-platform-linux]: http://img.shields.io/badge/-linux-2D3F6C.svg?style=flat
[badge-platform-macos]: http://img.shields.io/badge/-macos-111111.svg?style=flat
[badge-platform-ios]: http://img.shields.io/badge/-ios-CDCDCD.svg?style=flat
[badge-platform-tvos]: http://img.shields.io/badge/-tvos-808080.svg?style=flat
[badge-platform-watchos]: http://img.shields.io/badge/-watchos-C0C0C0.svg?style=flat
[badge-platform-wasm]: https://img.shields.io/badge/-wasm-624FE8.svg?style=flat
[badge-platform-windows]: http://img.shields.io/badge/-windows-4D76CD.svg?style=flat
[badge-support-android-native]: http://img.shields.io/badge/support-[AndroidNative]-6EDB8D.svg?style=flat
[badge-support-apple-silicon]: http://img.shields.io/badge/support-[AppleSilicon]-43BBFF.svg?style=flat
[badge-support-js-ir]: https://img.shields.io/badge/support-[js--IR]-AAC4E0.svg?style=flat
[badge-support-linux-arm]: http://img.shields.io/badge/support-[LinuxArm]-2D3F6C.svg?style=flat
