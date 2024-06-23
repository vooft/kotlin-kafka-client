import com.vanniktech.maven.publish.SonatypeHost

plugins {
    // core kotlin plugins
    alias(libs.plugins.kotlin.multiplatform)

    // publish
    alias(libs.plugins.dokka)
    alias(libs.plugins.maven.central.publish)
}

kotlin {
    jvm()

    js { nodejs() }
    wasmJs { nodejs() }

    macosArm64()
    linuxX64()

    iosArm64()
    iosSimulatorArm64()

    applyDefaultHierarchyTemplate()

    sourceSets {
        commonMain.dependencies {
            api(project(":client:highlevel"))
        }
    }
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
