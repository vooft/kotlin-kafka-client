plugins {
    `kotlin-library`
    `maven-central-publish`
}

kotlin {
    sourceSets {
        commonMain.dependencies {
            implementation(libs.kotlinx.serialization.core)
        }
    }
}

