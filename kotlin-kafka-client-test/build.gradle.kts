plugins {
    alias(libs.plugins.kotlin.multiplatform)
}

kotlin {
    jvm()

    applyDefaultHierarchyTemplate()

    sourceSets {
        commonMain.dependencies {
            implementation(libs.ktor.network)
            implementation(project(":kotlin-kafka-client-lowlevel"))
            implementation(project(":kotlin-kafka-client-core"))
        }

        jvmMain.dependencies {
            implementation("org.apache.kafka:kafka-clients:3.7.0")
            implementation("ch.qos.logback:logback-classic:1.5.6")
            implementation("org.slf4j:slf4j-api:2.0.13")
            implementation("org.testcontainers:kafka:1.19.8")
        }

    }
}
