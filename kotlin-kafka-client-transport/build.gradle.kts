plugins {
    `kotlin-library`
    `maven-central-publish`
}

kotlin {
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
