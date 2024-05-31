import kotlinx.benchmark.gradle.JvmBenchmarkTarget

plugins {
    alias(libs.plugins.kotlin.multiplatform)
    alias(libs.plugins.kotlinx.benchmark)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.kotlin.allopen)
}

allOpen {
    annotation("org.openjdk.jmh.annotations.State")
    annotation("kotlinx.benchmark.State")
}

kotlin {
    jvm()

    macosArm64()

    applyDefaultHierarchyTemplate()

    sourceSets {
        commonMain.dependencies {
            implementation(libs.kotlinx.benchmark.runtime)
            implementation(project(":kotlin-kafka-client-lowlevel"))
            implementation(project(":kotlinx-serialization-kafka"))
        }

        jvmMain.dependencies {
            implementation(libs.kotlin.reflect)
        }

        nativeMain { }
    }
}

benchmark {
    targets.register("jvm") {
        this as JvmBenchmarkTarget
        jmhVersion = "1.37" // TODO: remove once kotlinx.benchmark updates bundled JMH
    }

    targets.register("js")

    // not working for some reason
    targets.register("macosArm64")

    configurations {
        configureEach {
            advanced("jvmForks", "2")
        }
    }
}
