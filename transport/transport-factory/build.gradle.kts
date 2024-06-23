plugins {
    // core kotlin plugins
    alias(libs.plugins.kotlin.multiplatform)
    alias(libs.plugins.kotlin.serialization)

    // test plugins
    alias(libs.plugins.kotest.multiplatform)
}

kotlin {
    jvm()

    js { nodejs() }
    wasmJs { nodejs() }

    macosArm64()
    linuxX64()

    iosArm64()
    iosSimulatorArm64()

    applyDefaultHierarchyTemplate()

    sourceSets {
        commonMain.dependencies {
            api(project(":transport:transport-core"))
            implementation(libs.kotlinx.coroutines.core)
        }

        nativeMain.dependencies {
            implementation(project(":transport:transport-ktor"))
        }

        jvmMain.dependencies {
            implementation(project(":transport:transport-ktor"))
        }

        jsMain.dependencies {
            implementation(project(":transport:transport-nodejs"))
        }

        wasmJsMain.dependencies {
            implementation(project(":transport:transport-nodejs"))
        }
    }
}
