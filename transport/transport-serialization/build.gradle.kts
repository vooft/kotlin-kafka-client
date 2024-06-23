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

    macosArm64()
    linuxX64()

    applyDefaultHierarchyTemplate()

    sourceSets {
        commonMain.dependencies {
            implementation(project(":serialization:serialization-core"))
            implementation(libs.kotlinx.serialization.core)
            implementation(libs.kotlinx.io.core)
        }
    }
}
