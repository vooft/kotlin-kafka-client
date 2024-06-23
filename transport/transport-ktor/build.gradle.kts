plugins {
    // core kotlin plugins
    alias(libs.plugins.kotlin.multiplatform)
    alias(libs.plugins.kotlin.serialization)

    // test plugins
    alias(libs.plugins.kotest.multiplatform)
}

kotlin {
    jvm()

    macosArm64()
    linuxX64()

    iosArm64()
    iosSimulatorArm64()

    applyDefaultHierarchyTemplate()

    sourceSets {
        commonMain.dependencies {
            implementation(project(":transport:transport-core"))
            implementation(libs.ktor.network)
            implementation(libs.kotlinx.io.core)
            implementation(libs.kotlin.logging)
        }
    }
}
