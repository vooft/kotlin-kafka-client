plugins {
    // core kotlin plugins
    alias(libs.plugins.kotlin.multiplatform)
    alias(libs.plugins.kotlin.serialization)

    // test plugins
    alias(libs.plugins.kotest.multiplatform)
}

kotlin {

    js { nodejs() }
    wasmJs { nodejs() }

    applyDefaultHierarchyTemplate()

    sourceSets {
        commonMain.dependencies {
            implementation(project(":common:utils"))
            implementation(project(":transport:transport-core"))
            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlinx.io.core)
            implementation(libs.kotlin.logging)
        }
    }
}
