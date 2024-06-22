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

    applyDefaultHierarchyTemplate()

    sourceSets {
        commonMain.dependencies {
            implementation(project(":common:utils"))
            implementation(project(":transport:transport-core"))
            implementation(project(":transport:transport-serialization"))
            implementation(libs.ktor.network)
            implementation(libs.kotlinx.serialization.core)
            implementation(libs.kotlinx.io.core)
        }
    }
}