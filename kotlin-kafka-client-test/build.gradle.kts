import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    alias(libs.plugins.kotlin.multiplatform)
}

kotlin {
    jvm()

    macosArm64()

    applyDefaultHierarchyTemplate()

    sourceSets {
        commonMain.dependencies {
            implementation(libs.ktor.network)
            implementation(libs.kotlinx.uuid)
            implementation(project(":kotlin-kafka-client-lowlevel"))
            implementation(project(":kotlin-kafka-client-core"))
        }

        jvmMain.dependencies {
            implementation("org.apache.kafka:kafka-clients:3.7.0")
            implementation("ch.qos.logback:logback-classic:1.5.6")
            implementation("org.slf4j:slf4j-api:2.0.13")
            implementation("org.testcontainers:kafka:1.19.8")
        }

        commonTest.dependencies {
            implementation(libs.kotlin.test)
            implementation(libs.kotest.framework.engine)
            implementation(libs.kotest.assertions.core)
            implementation(libs.kotest.framework.datatest)
            implementation(libs.kotlin.reflect)
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
