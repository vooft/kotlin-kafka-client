import gradle.kotlin.dsl.accessors._e902d25e4acbe52760fceca8ba313b45.sourceSets
import org.jetbrains.kotlin.gradle.ExperimentalWasmDsl
import org.jetbrains.kotlin.gradle.internal.platform.wasm.WasmPlatforms.wasmJs

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
            implementation(project(":kotlin-kafka-client-lowlevel"))
            implementation(project(":kotlin-kafka-client-core"))
            implementation(project(":kotlin-kafka-client-transport"))
            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlin.logging)
        }

        jvmMain.dependencies {
            implementation(libs.kafka.clients)
            implementation(libs.logback.classic)
            implementation(libs.slf4j.api)
            implementation(libs.testcontainers.kafka)
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
