plugins {
    `kotlin-library`
    `maven-central-publish`
}

kotlin {
    sourceSets {
        commonMain.dependencies {
            api(project(":kotlin-kafka-client-common"))
            api(libs.kotlinx.io.core)

            implementation(project(":kotlin-kafka-client-lowlevel"))
            implementation(project(":kotlin-kafka-client-transport"))
            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlinx.datetime)
            implementation(libs.kotlin.logging)
        }
    }
}

