plugins {
    // core kotlin plugins
    alias(libs.plugins.kotlin.multiplatform)
    alias(libs.plugins.kotlin.serialization)

    // publish
    alias(libs.plugins.dokka)
    alias(libs.plugins.maven.central.publish)

    // test plugins
    alias(libs.plugins.kotest.multiplatform)
//    alias(libs.plugins.mokkery)
}

kotlin {
    jvm()

    macosArm64()
    linuxX64()

    iosArm64()
    iosSimulatorArm64()

    js { nodejs() }
    wasmJs { nodejs() }

    applyDefaultHierarchyTemplate()

    sourceSets {
        commonMain.dependencies {
        }
    }
}
