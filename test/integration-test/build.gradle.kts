import org.jetbrains.kotlin.gradle.targets.js.dsl.ExperimentalWasmDsl

plugins {
    `kotlin-base`
}

kotlin {
    jvm()

    js {
        nodejs {
            testTask {
                useMocha {
                    timeout = "2m"
                }
            }
        }
        binaries.executable()
    }

    @OptIn(ExperimentalWasmDsl::class)
    wasmJs {
        nodejs {
            testTask {
                useMocha {
                    timeout = "2m"
                }
            }
        }
        binaries.executable()
    }

    applyDefaultHierarchyTemplate()

    sourceSets {
        commonMain.dependencies {
            implementation(libs.kt.uuid)
            implementation(project(":kotlin-kafka-client-lowlevel"))
            implementation(project(":kotlin-kafka-client-core"))
            implementation(project(":kotlin-kafka-client-transport"))
            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlin.logging)
        }

        jvmMain.dependencies {
            implementation("org.apache.kafka:kafka-clients:3.8.0")
            implementation("ch.qos.logback:logback-classic:1.5.14")
            implementation("org.slf4j:slf4j-api:2.0.16")
            implementation("org.testcontainers:kafka:1.20.2")
            implementation(libs.kotlinx.io.core)
        }

        commonTest.dependencies {
            implementation(libs.kotlinx.io.core)
            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlinx.coroutines.test)

            implementation(libs.kotlin.test)
            implementation(libs.kotest.assertions.core)
            implementation(libs.kotlin.reflect)
        }

        jvmTest.dependencies {
            implementation(libs.ktor.network)
        }
    }
}
