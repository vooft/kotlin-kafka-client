plugins {
    `kotlin-dsl`
}

repositories {
    mavenCentral()
}

dependencies {
    // kotlin
    implementation("org.jetbrains.kotlin:kotlin-gradle-plugin:${libs.versions.kotlin.get()}")
    implementation("org.jetbrains.kotlin:kotlin-serialization:${libs.versions.kotlin.get()}")

    // detekt
    implementation("io.gitlab.arturbosch.detekt:detekt-gradle-plugin:${libs.plugins.detekt.get().version}")

    // publishing
    implementation("org.jetbrains.dokka:dokka-gradle-plugin:${libs.plugins.dokka.get().version}")
    implementation("com.vanniktech:gradle-maven-publish-plugin:${libs.plugins.maven.central.publish.get().version}")
}

