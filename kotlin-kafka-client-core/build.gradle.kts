plugins {
    id("kotlin-library")
    id("maven-central-publish")
}

kotlin {
    sourceSets {
        commonMain.dependencies {
            implementation(project(":kotlin-kafka-client-lowlevel"))
            implementation(project(":kotlin-kafka-client-transport"))
            implementation(libs.kotlinx.io.core)
            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlinx.datetime)
            implementation(libs.kotlin.logging)
        }
    }
}

