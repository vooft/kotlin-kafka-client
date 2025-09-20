import io.gitlab.arturbosch.detekt.Detekt

plugins {
    kotlin("multiplatform")
    kotlin("plugin.serialization")
    id("io.gitlab.arturbosch.detekt")
}

detekt {
    buildUponDefaultConfig = true
    config.from(files("$rootDir/detekt.yml"))
    basePath = rootDir.absolutePath
}

tasks.withType<Detekt> {
    tasks.getByName("check").dependsOn(this)
}

kotlin {
    tasks.withType<AbstractTestTask> {
        testLogging {
            showExceptions = true
            showStandardStreams = true
            events = setOf(
                org.gradle.api.tasks.testing.logging.TestLogEvent.FAILED,
                org.gradle.api.tasks.testing.logging.TestLogEvent.PASSED
            )
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        }
    }

    compilerOptions {
        optIn.addAll(
            "kotlin.uuid.ExperimentalUuidApi"
        )
    }
}
