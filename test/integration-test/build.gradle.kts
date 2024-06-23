import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    alias(libs.plugins.kotlin.multiplatform)
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
            implementation(project(":client:lowlevel"))
            implementation(project(":kotlin-kafka-client-core"))
            implementation(project(":transport:transport-factory"))
            implementation(project(":serialization:serialization-core"))
            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlin.logging)
        }

        jvmMain.dependencies {
            implementation("org.apache.kafka:kafka-clients:3.7.0")
            implementation("ch.qos.logback:logback-classic:1.5.6")
            implementation("org.slf4j:slf4j-api:2.0.13")
            implementation("org.testcontainers:kafka:1.19.8")
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

    // TODO: move to buildSrc
    tasks.named<Test>("jvmTest") {
        useJUnitPlatform()
    }

    afterEvaluate {
        tasks.withType<AbstractTestTask> {
            testLogging {
                showExceptions = true
                showStandardStreams = true
                events = setOf(TestLogEvent.STARTED, TestLogEvent.FAILED, TestLogEvent.PASSED)
                exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
            }
        }
    }
}
