import com.vanniktech.maven.publish.SonatypeHost

plugins {
    id("org.jetbrains.dokka")
    id("com.vanniktech.maven.publish")
}

mavenPublishing {
    publishToMavenCentral(SonatypeHost.CENTRAL_PORTAL)

    signAllPublications()

    pom {
        name = "kotlin-kafka-client"
        description = "Kotlin Multiplatform implementation of Kafka Client"
        url = "https://github.com/vooft/kotlin-kafka-client"
        licenses {
            license {
                name = "The Apache License, Version 2.0"
                url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
            }
        }
        scm {
            connection = "https://github.com/vooft/kotlin-kafka-client"
            url = "https://github.com/vooft/kotlin-kafka-client"
        }
        developers {
            developer {
                name = "kotlin-kafka-client team"
            }
        }
    }
}
