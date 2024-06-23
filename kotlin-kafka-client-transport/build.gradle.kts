plugins {
    // core kotlin plugins
    alias(libs.plugins.kotlin.multiplatform)
    alias(libs.plugins.kotlin.serialization)

    // test plugins
    alias(libs.plugins.kotest.multiplatform)
//    alias(libs.plugins.mokkery)
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
            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlinx.io.core)
            implementation(libs.kotlin.logging)
        }

        jvmMain.dependencies {
            implementation(libs.ktor.network)
        }

        nativeMain.dependencies {
            implementation(libs.ktor.network)
        }
    }
}
