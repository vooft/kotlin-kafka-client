plugins {
    alias(libs.plugins.kotlin.multiplatform) apply false
    alias(libs.plugins.detekt)
}

allprojects {
    group = "io.github.vooft"
    version = System.getenv("TAG") ?: "1.0-SNAPSHOT"

    apply(plugin = "io.gitlab.arturbosch.detekt")

    repositories {
        mavenCentral()
    }

    detekt {
        buildUponDefaultConfig = true
        config.from(files("$rootDir/detekt.yml"))
        basePath = rootDir.absolutePath

        source.setFrom("src/commonMain/kotlin", "src/commonTest/kotlin", "src/jvmTest/kotlin")
    }
}

