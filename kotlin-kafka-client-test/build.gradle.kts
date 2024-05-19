plugins {
    alias(libs.plugins.kotlin.multiplatform)
}

kotlin {
    jvm()

    applyDefaultHierarchyTemplate()

    sourceSets {
        commonMain.dependencies {
            implementation(libs.ktor.network)
        }

        jvmMain.dependencies {
            implementation("org.apache.kafka:kafka-clients:3.7.0")
            implementation("ch.qos.logback:logback-classic:1.3.5")
            implementation("org.slf4j:slf4j-api:2.0.4")
        }

    }
}
