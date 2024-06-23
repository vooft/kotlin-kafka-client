plugins {
    id("kotlin-library")
    id("maven-central-publish")
}

kotlin {
    sourceSets {
        commonMain.dependencies {
            implementation(libs.kotlinx.serialization.core)
        }
    }
}

