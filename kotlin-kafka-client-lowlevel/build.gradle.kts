plugins {
    `kotlin-library`
    `maven-central-publish`
}

kotlin {
    sourceSets {
        commonMain.dependencies {
            implementation(libs.kotlinx.serialization.core)
            implementation(libs.kotlinx.io.core)
            implementation(project(":kotlin-kafka-client-common"))
            implementation(project(":kotlin-kafka-client-transport"))
        }

        jvmMain.dependencies { }

//        jsMain.dependencies { }

        commonTest.dependencies {
            implementation(libs.kotlin.test)
            implementation(libs.kotest.assertions.core)
            implementation(libs.kotlin.reflect)
            implementation(libs.kotlinx.coroutines.test)
        }

        jvmTest.dependencies {
        }
    }
}
