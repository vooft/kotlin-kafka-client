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

    macosArm64()
    linuxX64()

    applyDefaultHierarchyTemplate()

    sourceSets {
        commonMain.dependencies {
            implementation(libs.kotlinx.io.core)
            implementation(libs.kotlinx.serialization.core)
            implementation(project(":kotlin-kafka-client-common"))
            implementation(project(":kotlinx-serialization-kafka-common"))
//            implementation(libs.canard)
//            implementation(project(":kotlin-kafka-client-common"))
//            implementation(project(":serialization:kotlinx-serialization-kafka")
        }
    }
}
